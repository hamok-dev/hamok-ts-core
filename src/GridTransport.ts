import { 
    Message, 
    createLogger,
} from "@hamok-dev/common"


const logger = createLogger("GridTransport");

// export type MessageBytesListener = (bytes: Uint8Array) => void;
export type MessageListener = (message: Message) => void;
export interface GridTransport {
    readonly receiver: MessageListener;
    sender: MessageListener | undefined;
    send(message: Message): void;
    receive(message: Message): void;
    // sender(listener: MessageBytesListener): void;
}

export abstract class GridTransportAbstract implements GridTransport {
    private _sender?: MessageListener;
    private _noSenderAvailable = false;

    public set sender(listener: MessageListener | undefined) {
        if (this._sender && listener !== undefined) {
            logger.warn("sender is assigned more than once. Only the last assign will be used");
        }
        this._sender = listener;
        this._noSenderAvailable = this._sender !== undefined;
    }

    public send(message: Message): void {
        if (!this._sender) {
            if (!this._noSenderAvailable) {
                logger.warn(`No sender is available to send messages on transport`);
                this._noSenderAvailable = true;
            }
            return;
        }
        if (!this.canSend(message)) {
            return;
        }
        try {
            try {
                this._sender(message);
            } catch (err) {
                logger.warn(`Error occurred while sending message`, err);
            }
        } catch (err) {
            logger.warn(`Error occurred while serializing message. `, message, err);
        }
    }

    protected canSend(message: Message): boolean {
        return true;
    }

    public get receiver(): MessageListener {
        return (message: Message) => {
            this.receive(message);
        }
    }

    public abstract receive(message: Message): void;
}