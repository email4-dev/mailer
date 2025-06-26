import PocketBase from 'pocketbase'
import Valkey from 'iovalkey'
import * as Minio from 'minio'
import { mail, initTransporter } from "./mailer"
import { build } from './builder'
import { setTimeout } from 'node:timers/promises'
import type Mail from 'nodemailer/lib/mailer'

const db = new PocketBase(Bun.env.POCKETBASE_URL)

db.autoCancellation(false)

const file = Bun.file('package.json')
const content = await file.text()
const match = content.match(/"version"\s*:\s*"([^"]+)"/)
const version = match ? match[1] : '0.0.0'
const serviceName = Bun.argv.includes('--retrier') ? 'Retry Mailer' : 'Mailer'

console.log(`Email4.dev ${serviceName} Service v${version} starting...`)

const smtpOptions: SMTPConnectionOptions = {
    hostname: Bun.env.SMTP_HOSTNAME || '',
    port: parseInt(Bun.env.SMTP_PORT || '465'),
    security: (Bun.env.SMTP_SECURITY as SMTPConnectionOptions["security"]) || 'ssl',
    auth: (Bun.env.SMTP_AUTH as SMTPConnectionOptions["auth"]) || 'plain',
    username: Bun.env.SMTP_USERNAME || '',
    password: Bun.env.SMTP_PASSWORD || '',
}

if(!smtpOptions.hostname || !smtpOptions.username || !smtpOptions.password) {
    throw('SMTP variables missing!')
}

if(Bun.env.SMTP_PRIVATE_KEY) smtpOptions.private_key = Bun.env.SMTP_PRIVATE_KEY
if(Bun.env.SMTP_ACCESS_URL) smtpOptions.access_url = Bun.env.SMTP_ACCESS_URL

const transporter = initTransporter(smtpOptions)

await db.collection('_superusers').authWithPassword(
    Bun.env.POCKETBASE_EMAIL!,
    Bun.env.POCKETBASE_PASS!,
)

if(!db.authStore.isValid) {
    throw('Pocketbase authentication failed!')
}

const valkey = new Valkey(6379, 'valkey') // non-blocking connection
const sub = new Valkey(6379, 'valkey') // main worker connection
const consumerName = Bun.argv.includes('--retrier') ? `retrier-${process.pid}` : `mailer-${process.pid}`
const consumerGroup = Bun.argv.includes('--retrier') ? 'retrier-group' : 'mailer-group'
const stream = Bun.argv.includes('--retrier') ? 'retry_queue' : 'messages'
const minio = new Minio.Client({
    endPoint: 'minio',
    port: 9000,
    accessKey: Bun.env.MINIO_ROOT_USER!,
    secretKey: Bun.env.MINIO_ROOT_PASSWORD!,
    useSSL: false,
    pathStyle: true
})

valkey.on("error", (err) => {
    if(err) {
        throw err
    } else {
        throw 'Valkey disconnected'
    }
})

process.on("beforeExit", async () => {
    console.log('Email4.dev Mailer exiting gracefully...')
    transporter.close()
    db.authStore.clear()
    await valkey.quit()
    await sub.quit()
})

const streamExists = await sub.exists(stream)

if(streamExists === 0) {
    throw `${stream} stream does not exist!`
}

try {
    await sub.xgroup('CREATE', stream, consumerGroup, '0')
} catch (err) {
    if (!(err as Error).message.includes('BUSYGROUP')) {
        throw err
    }
}

const parseRedisMessage = (id:string, message: string[]) => {
    let result: Partial<Message> = { id }
    for (let i=0; i<message.length; i+=2) {
        switch(message[i]) {
            case 'hex':
                result.hex = message[i+1]
                break
            case 'form_id':
                result.form_id = message[i+1]
                break
            case 'fields':
                try {
                    result.fields = JSON.parse(message[i+1])
                } catch(error) {
                    console.warn(`Invalid message format for message ${id}`, message)
                    return null
                }
                break
            case 'origin':
                result.origin = message[i+1]
                break
            case 'attachment_count':
                result.attachment_count = parseInt(message[i+1])
                if(isNaN(result.attachment_count)) {
                    console.warn(`Invalid message format for message ${id}`, message)
                    return null
                }
                break
            case 'fail_count':
                result.fail_count = parseInt(message[i+1])
                if(isNaN(result.fail_count)) {
                    console.warn(`Invalid message format for message ${id}`, message)
                    return null
                }
                break
        }
    }
    return result as Message
}

const pendingMessages = await sub.xpending(stream, consumerGroup)
// @ts-expect-error
const pendingMessageCount = parseInt(pendingMessages[0]) || 0
if(pendingMessageCount > 0) {
    const idleTime = Bun.argv.includes('--retrier') ? 5_400_000 : 300_000 // 90 minutes for retry_queue, 5 minutes for messages queue
    const entries: any[] = await sub.xautoclaim(stream, consumerGroup, consumerName, idleTime, '0-0')
    if(Bun.env.DEBUG == "true") console.debug('Reading pending entries', entries)
    if(!entries || !entries[1].length) {
        console.info('No pending entries found.')
    } else {
        console.info(`Found ${entries[1].length} pending entries. Processing those first...`)
        await processStream(entries[1])
    }
}

while(true) {
    const entries: any[] = await sub.xreadgroup('GROUP', consumerGroup, consumerName, 'COUNT', (parseInt(Bun.env.CONSUMER_BATCH_SIZE!) || 5), 'BLOCK', (parseInt(Bun.env.CONSUMER_BLOCK!) || 10) * 1000, 'STREAMS', stream, '>')
    if(!entries || !entries[0].length || !entries[0][1].length) continue
    if(Bun.env.DEBUG == "true") console.debug('Reading entries', entries)
    await processStream(entries[0][1])
}

async function processStream(entries: any[]) {
    for(const stream of entries) {
        if(Bun.env.DEBUG == "true") console.debug(`Reading message ${stream[0]}`, stream[1])
        const message = parseRedisMessage(stream[0], stream[1])
        if(message){
            if(Bun.argv.includes('--retrier')) {
                (async () => {
                    await setTimeout(message.fail_count! * (parseInt(Bun.env.RETRY_INTERVAL!) || 15) * 60 * 1000) // 15/30/45/60/75 minutes
                    await processMessage(message)
                })()
            } else {
                await processMessage(message)
            }
        } else {
            const attachment_count = parseInt(stream[1][8]) || 0
            if(attachment_count > 0) await clearAttachments(stream[1][1])
            await valkey.lpush('failed', ...stream[1])
            await valkey.del(`streams:${stream[1][1]}`)
            await valkey.xdel('messages', stream[0])
        }
    }
}

async function clearAttachments(hex: string) {
    const attachments = await valkey.hgetall(`attachments:${hex}`)
    if(attachments) {
        const files:MessageAttachment[] = JSON.parse(attachments.files) || []
        if(files.length) await minio.removeObjects('attachments', files.map(a => a.key))
        await valkey.del(`attachments:${hex}`)
    }
}

async function processMessage(message: Message) {
    if(Bun.env.DEBUG == "true") console.debug(`New message via ${message.origin} for form ${message.form_id} with ${message.attachment_count} attachments and fields:`, message.fields)

    const form = await db.collection('forms').getOne(message.form_id!, {
        expand: 'handler,handler.template',
        fields: '*,expand.handler.*,expand.handler.template.*'
    }).then(data => data).catch(() => null)

    if(!form) {
        await valkey.lpush('failed', 'hex', message.hex, 'form_id', message.form_id, 'fields', JSON.stringify(message.fields), 'origin', message.origin, 'attachment_count', message.attachment_count, 'error', 'form not found')
        await valkey.del(`streams:${message.hex}`)
        await valkey.xdel('messages', message.id)
        await clearAttachments(message.hex)
        return
    }

    let email: Error | Mail.Options

    if(message.hex === 'otp') {
        email = {
            subject: `OTP Code: ${message.fields[0].value}`,
            html: `<p>You have requested access to protected email4.dev resources.</p><p>Your OTP code is: <strong>${message.fields[0].value}</strong></p>`,
            text: `You have requested access to protected email4.dev resources.\n\nYour OTP code is:\n${message.fields[0].value}`,
            from: form.expand?.handler.from_name.length ? `${form.expand?.handler.from_name} <${form.expand?.handler.from_email}>` : form.expand?.handler.from_email,
            to: form.expand?.handler.to,
        }
    } else {
        email = await build(form, message.fields, message.origin, message.attachment_count > 0 ? `${Bun.env.API_URL}attachments/${message.hex}` : null)
    }

    if(email instanceof Error) {
        console.warn(email.message)
        if(!form?.allow_duplicates) await valkey.del(`streams:${message.hex}`)
        if(message.attachment_count) await clearAttachments(message.hex)
        await valkey.xdel('messages', message.id)
        return
    }

    const smtpResult: Boolean | Error = await mail(email, message.hex, transporter)
    
    if(smtpResult instanceof Error) {
        console.warn(smtpResult.message)
        if(!form?.allow_duplicates) await valkey.del(`streams:${message.hex}`)
        if(message.attachment_count) await clearAttachments(message.hex)
        await valkey.xdel('messages', message.id)
        return
    }

    if(smtpResult === false) {
        if(Bun.argv.includes('--retrier')) {
            if(message.fail_count! >= (parseInt(Bun.env.MAILER_RETRIES!) || 5)) {
                await valkey.lpush('failed', 'hex', message.hex, 'form_id', message.form_id, 'fields', JSON.stringify(message.fields), 'origin', message.origin, 'attachment_count', message.attachment_count, 'error', 'max retries reached')
            } else {
                await valkey.xadd('retry_queue', message.id, 'hex', message.hex, 'form_id', message.form_id, 'fields', JSON.stringify(message.fields), 'origin', message.origin, 'attachment_count', message.attachment_count, 'fail_count', message.fail_count! + 1)
            }
        } else {
            await valkey.xadd('retry_queue', message.id, 'hex', message.hex, 'form_id', message.form_id, 'fields', JSON.stringify(message.fields), 'origin', message.origin, 'attachment_count', message.attachment_count, 'fail_count', '1')
        }
    }

    if(!form?.allow_duplicates) await valkey.del(`streams:${message.hex}`)
    await valkey.xdel('messages', message.id)
}