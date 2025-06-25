import mjml2html from 'mjml'
import type { RecordModel } from 'pocketbase'
import type { SendMailOptions } from 'nodemailer'

const mjmlOptions = {
    keepComments: false,
    minify: true,
}

export const build = async (form: RecordModel|null, fields: MessageField[], origin: string, downloadUrl: string|null = null) => {
    if(form === null) {
        return new Error('Form was not found!')
    } else {
        if(Bun.env.DEBUG == "true") console.debug(`Form ${form.id} data: ${JSON.stringify(form)}`)

        if(form.handler === null) {
            return new Error(`Form with id ${form.id} has no linked handler!`)
        }

        let html:string = ''
        let text:string = ''
        let subject:string = ''

        const fieldGroups = fields.reduce((acc: {[key: string]: string[]}, field) => {
            const normalizedName = field.name.replace('[]', '')
            if (!acc[normalizedName]) acc[normalizedName] = []
            acc[normalizedName].push(field.value)
            return acc
        }, {})

        if(form.expand?.handler.expand?.template) {
            html = form.expand?.handler.expand?.template.type === 'mjml' ? mjml2html(form.expand?.handler.expand?.template.code, mjmlOptions) : form.expand?.handler.expand?.template.code
            text = form.expand?.handler.expand?.template.text
            subject = form.expand?.handler.expand?.template.subject
            for (const [name, values] of Object.entries(fieldGroups)) {
                html = html.replaceAll(`{${name}}`, values.join(', '))
                text = text.replaceAll(`{${name}}`, values.join(', '))
                subject = subject.replaceAll(`{${name}}`, values.join(', '))
            }
        } else {
            subject = `New message via ${origin}`
            html = '<table style="border-collapse: collapse; width: 600px;" border="1" cellspacing="5" cellpadding="5"><tbody>'
            for (const [name, values] of Object.entries(fieldGroups)) {
                html += `<tr><th width="33.3333%">${name}</th><td width="66.6666%">${values.join(', ')}</td></tr>`
                text += `${name}\n${values.join(', ')}\n\n`
            }
            html += '</tbody></table>'
        }

        html = html.replaceAll(/\r?\n|\r/g, '<br>')

        if(!subject.length) {
            return new Error(`Form with id ${form.id} has no subject!`)
        }

        if(downloadUrl) {
            html += `<div style="padding:10px;margin:15px 0;background-color:#eeeeee;">This message has attachments! View them <a href="${downloadUrl}">here</a>.</div>`
            text += `\n\nThis message has attachments! View them at ${downloadUrl}`
        }

        if(!html.length && !text.length) {
            return new Error(`Submission for form with id ${form.id} has no content!`)
        }

        let replyTo:string = ''
        if(form.expand?.handler.reply_to.length) {
            if(form.expand?.handler.reply_to.indexOf('{') > -1) {
                const replyToFieldName: string = form.expand?.handler.reply_to.replaceAll(/{|}/, '')
                const replyToField = fields.find(f => f.name === replyToFieldName)
                if(replyToField) replyTo = form.expand?.handler.reply_to.replace(`{${replyToFieldName}}`, replyToField.value)
            } else {
                replyTo = form.expand?.handler.reply_to
            }
        }

        const email:SendMailOptions = {
            subject,
            html,
            text,
            from: form.expand?.handler.from_name.length ? `${form.expand?.handler.from_name} <${form.expand?.handler.from_email}>` : form.expand?.handler.from_email,
            to: form.expand?.handler.to,
        }

        if(replyTo.length) email.replyTo = replyTo

        return email
    }
}