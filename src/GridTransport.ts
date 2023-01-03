import { 
    Message, 
    MessageType, 
    MessageDefaultProcessor, 
    MessageProcessor, 
    GridCodec, 
    createLogger,
    HelloNotification,
    SubmitMessageRequest,
    EndpointStatesNotification,
    StorageSyncRequest,
    StorageSyncResponse,
    RaftVoteRequest,
    RaftVoteResponse,
    RaftAppendEntriesRequestChunk,
    RaftAppendEntriesResponse,
    SubmitMessageResponse,
} from "@hamok-dev/common"

import { EventEmitter } from "ws";

const logger = createLogger("GridTransport");

export type MessageBytesListener = (bytes: Uint8Array) => void;

export interface GridTransport {
    readonly receiver: MessageBytesListener;
    sender: MessageBytesListener;
    // sender(listener: MessageBytesListener): void;
}

export abstract class GridTransportAbstract implements GridTransport {
    private _sender?: MessageBytesListener;
    private _noSenderAvailable = false;

    public set sender(listener: MessageBytesListener) {
        if (this._sender) {
            logger.warn("sender is assigned more than once. Only the last assign will be used");
        }
        this._sender = listener;
        this._noSenderAvailable = false;
    }

    public send(message: Message): void {
        if (!this._sender) {
            if (!this._noSenderAvailable) {
                logger.warn(`No sender is available to send messages on transport`);
                this._noSenderAvailable = true;
            }
            return;
        }
        try {
            const bytes = message.toBinary();
            try {
                this._sender(bytes);
            } catch (err) {
                logger.warn(`Error occurred while sending message`, err);
            }
        } catch (err) {
            logger.warn(`Error occurred while serializing message. `, message, err);
        }
    }

    public get receiver(): MessageBytesListener {
        return (bytes) => {
            let message: Message | undefined;
            try {
                message = Message.fromBinary(bytes);
            } catch (err) {
                logger.warn(`Error occurred while deserializing message`, err);
                return;
            }
            if (!message) {
                return;
            }
            this.receive(message);
        }
    }

    protected abstract receive(message: Message): void;
}