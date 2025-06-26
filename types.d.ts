interface Message {
    id: string;
    hex: string;
    form_id: string;
    origin: string;
    fields: MessageField[];
    attachment_count: number;
    fail_count?: number;
}

interface MessageField {
    name: string;
    value: any;
}

interface MessageAttachment {
    name: string;
    key: string;
    filename: string;
}

interface SMTPConnectionOptions {
    hostname: string;
    port: number;
    security: 'none' | 'starttls' | 'ssl';
    auth: 'gmail' | 'oauth2' | 'plain';
    username: string;
    password: string;
    private_key?: string;
    access_url?: string;
}