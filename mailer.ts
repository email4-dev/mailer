import nodemailer, { type SendMailOptions } from "nodemailer"
import type SMTPConnection from "nodemailer/lib/smtp-connection"
import type SMTPPool from "nodemailer/lib/smtp-pool"

export const initTransporter = (options: SMTPConnectionOptions) => {
    const connectionInfo: SMTPConnection.Options | SMTPPool.Options = {
        host: options.hostname,
        port: options.port,
        secure: options.security === 'ssl',
        auth: {},
    }

    if(Bun.env.SMTP_POOL === "true") (connectionInfo as SMTPPool.Options).pool = true

    switch(options.auth) {
        case 'gmail':
            connectionInfo.auth = {
                type: "OAUTH2",
                user: options.username,
                serviceClient: options.password,
                privateKey: options.private_key,
            }
            break
        case 'oauth2':
            connectionInfo.auth = {
                type: "OAUTH2",
                user: options.username,
                accessToken: options.password,
                accessUrl: options.access_url,
            }
            break
        default: // plain
            connectionInfo.auth = {
                user: options.username,
                pass: options.password,
            }
            break
    }
    return nodemailer.createTransport(connectionInfo)
}

export const mail = async (email: SendMailOptions, messageId: string, transporter: nodemailer.Transporter|null = null, options?: SMTPConnectionOptions) => {
    if(!transporter) {
        if(options) {
            transporter = initTransporter(options)
        } else {
            console.warn(`Error sending ${messageId}: Neither transporter not SMTP options are set.`)
            return false
        }
    }

    let failed = false

    transporter?.sendMail(email, async (error, info) => {
        if(error) {
            console.warn(`Error sending ${messageId}: ${error.message}`)
            failed = true
        } else {
            for (const rejected of info.rejected) {
                console.warn(`Message ${info.messageId} was rejected by ${rejected}`)
            }
            if(Bun.env.DEBUG == "true") console.debug(`Message ${info.messageId} was sent`)
        }
    })

    if(Bun.env.SMTP_POOL !== "true") transporter?.close()

    return !failed
}