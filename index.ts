import { connect } from "@nats-io/transport-node"
import { AckPolicy, DeliverPolicy, jetstream, jetstreamManager, StorageType, type JsMsg } from "@nats-io/jetstream"
import { Objm } from "@nats-io/obj"
import nodemailer, {type SendMailOptions} from "nodemailer"
import PocketBase from 'pocketbase'
import mjml2html from 'mjml'
import type { Attachment } from "nodemailer/lib/mailer"
import { Readable } from "stream"

const mjmlOptions = {
    keepComments: false,
    minify: true,
}

interface NatsMessage { form_id: string, fields: {name: string, value: string}[], attachments: {name: string, key: string}[]}

const db = new PocketBase(Bun.env.POCKETBASE_URL)

console.log('Email4.dev Mailer Service starting...')

await db.collection('_superusers').authWithPassword(
    Bun.env.POCKETBASE_EMAIL!,
    Bun.env.POCKETBASE_PASS!,
)

if(!db.authStore.isValid) {
    throw('Pocketbase authentication failed!')
}

// helper functions
function objectToNodeStream(objectStream: ReadableStream<Uint8Array>): Readable {
  const reader = objectStream.getReader()
  return new Readable({
    async read() {
      const { done, value } = await reader.read()
      if (done) {
        this.push(null)
      } else {
        this.push(value)
      }
    }
  })
}

function giveUp(msg: JsMsg, error: string) {
    console.warn(error)
    msg.term(error)
}

const nc = await connect({ servers: Bun.env.NATS_HOST })
const js = await jetstream(nc)
const jsm = await jetstreamManager(nc)
const objm = new Objm(nc)

const max_messages = parseInt(Bun.env.MAX_MESSAGES!) || 10
const expires = parseInt(Bun.env.INTERVAL!) * 1000 || 10_000

nc.closed().then((err) => {
    if(err) {
        console.error('NATS disconnected', err.message)
    } else {
        console.error('NATS disconnected')
    }
    process.exit(1)
})

process.on("beforeExit", async () => {
    console.log('Email4.dev Mailer exiting gracefully...')
    clearInterval(interval)
    db.authStore.clear()
    await nc.drain()
})

try {
    const consumerInfo = await jsm.consumers.info("messages", "mailer")
} catch (err: any) {
    console.warn('Consumer not found, creating it...')
    await jsm.consumers.add("messages", {
        durable_name: "mailer",
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        ack_wait: 60 * 1000, // 1 minute
    })
}

const c = await js.consumers.get("messages", "mailer")
const bucket = await objm.create("attachments", { storage: StorageType.File })
let sub = await c.consume({ max_messages, expires })
const interval = setInterval(async() => sub = await c.consume({ max_messages }), expires)

for await (const message of sub) {
    message.working()

    const { form_id, fields, attachments } = message.json() as NatsMessage

    if(Bun.env.DEBUG == "true") console.debug(`New message for form ${form_id} with ${attachments.length} attachments and fields: ${JSON.stringify(fields)}`)

    const form = await db.collection('forms').getOne(form_id, {
        expand: 'handler,handler.template,handler.gateway',
        fields: '*,expand.handler.*,expand.handler.template.*,expand.handler.gateway.*'
    }).then(data => data).catch(() => null)

    if(Bun.env.DEBUG == "true") console.debug(`Form ${form_id} data: ${JSON.stringify(form)}`)

    if(form === null) {
        giveUp(message, `Form with id ${form_id} was not found!`)
        continue
    }

    if(form.handler === null) {
        giveUp(message, `Form with id ${form_id} has no linked handler!`)
        continue
    }

    if(form.expand?.handler.expand?.template === null) {
        giveUp(message, `Form with id ${form_id} has no linked template!`)
        continue
    }

    if(form.expand?.handler.expand?.gateway === null) {
        giveUp(message, `Handler of form with id ${form_id} has no linked gateway!`)
        continue
    }

    let html:string = form.expand?.handler.expand?.template.type === 'mjml' ? mjml2html(form.expand?.handler.expand?.template.code, mjmlOptions) : form.expand?.handler.expand?.template.code
    let text:string = form.expand?.handler.expand?.template.text
    let subject:string = form.expand?.handler.expand?.template.subject
    let replyTo:string|null = null
    if(form.expand?.handler.reply_to.length) {
        if(form.expand?.handler.reply_to.indexOf('{') > -1) {
            const replyToField = form.expand?.handler.reply_to.replaceAll(/{|}/, '')
            const replyToValue = fields.find(f => f.name === replyToField)
            if(replyToValue) replyTo = form.expand?.handler.reply_to.replace(`{${replyToField}}`, replyToValue)
        } else {
            replyTo = form.expand?.handler.reply_to
        }
    }

    for (const field of fields) {
        html = html.replaceAll(`{${field.name}}`, field.value).replaceAll(/\r?\n|\r/g, '<br>')
        text = text.replaceAll(`{${field.name}}`, field.value)
        subject = subject.replaceAll(`{${field.name}}`, field.value)
    }

    const email:SendMailOptions = {
        subject,
        from: form.expand?.handler.from_name.length ? `${form.expand?.handler.from_name} <${form.expand?.handler.from_email}>` : form.expand?.handler.from_email,
        to: form.expand?.handler.to,
    }

    if(html.length) email.html = html
    if(text.length) email.text = text
    if(replyTo) email.replyTo = replyTo

    if(attachments.length) {
        email.attachments = await Promise.all(attachments.map(async a => {
            const file = await bucket.get(a.key)
            if(file?.error) {
                console.warn(`Error loading attachment ${a.key}: ${file?.error}`)
                return null
            } else {
                return {
                    filename: `${a.name}_${a.filename}`,
                    content: objectToNodeStream(file!.data)
                } as Attachment
            }
        }).filter(x => x)) as Attachment[]
    }

    const message_id = message.headers?.get('Nats-Msg-Id')

    const connectionInfo = {
        host: form.expand?.handler.expand?.gateway.hostname,
        port: form.expand?.handler.expand?.gateway.port,
        secure: form.expand?.handler.expand?.gateway.security === 'ssl',
        auth: {},
    }

    switch(form.expand?.handler.expand?.gateway.auth) {
        case 'gmail':
            connectionInfo.auth = {
                type: "OAuth2",
                user: form.expand?.handler.expand?.gateway.username,
                serviceClient: form.expand?.handler.expand?.gateway.password,
                privateKey: form.expand?.handler.expand?.gateway.private_key,
            }
            break
        case 'oauth2':
            connectionInfo.auth = {
                type: "OAuth2",
                user: form.expand?.handler.expand?.gateway.username,
                accessToken: form.expand?.handler.expand?.gateway.password,
                accessUrl: form.expand?.handler.expand?.gateway.access_url,
                }
            break
        default: // plain
            connectionInfo.auth = {
                user: form.expand?.handler.expand?.gateway.username,
                pass: form.expand?.handler.expand?.gateway.password,
            }
            break
    }

    const transporter = nodemailer.createTransport(connectionInfo)
    await transporter.sendMail(email, (error, info) => {
        if(error) {
            giveUp(message, `Error sending ${message_id}: ${error.message}`)
        } else {
            // message_id leaves the queue, use info.messageId, which is the id assigned by the MTA from now on
            for (const rejected of info.rejected) {
                console.warn(`Message ${info.messageId} was rejected by ${rejected}`)
            }
            if(Bun.env.DEBUG == "true") console.debug(`Message ${info.messageId} was sent`)
            message.ack()
        }
    })

    transporter.close()

    attachments.forEach(async a => await bucket.delete(a.key))

    if(message.info.pending === 0) break
}