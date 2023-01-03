import { 
    Message, 
    MessageType, 
    MessageDefaultProcessor, 
    MessageProcessor, 
    GridCodec, 
    createLogger,
    SubmitMessageRequest,
    StorageSyncRequest,
    StorageSyncResponse,
    SubmitMessageResponse,
} from "@hamok-dev/common"
import { EventEmitter } from "ws";

const logger = createLogger("GridTransport");

const OUTBOUND_MESSAGE = "OUTBOUND_MESSAGE";
const STORAGE_SYNC_REQUEST = MessageType.STORAGE_SYNC_REQUEST.toString();
const STORAGE_SYNC_RESPONSE = MessageType.STORAGE_SYNC_RESPONSE.toString();
const SUBMIT_MESSAGE_REQUEST = MessageType.SUBMIT_MESSAGE_REQUEST.toString();
const SUBMIT_MESSAGE_RESPONSE = MessageType.SUBMIT_MESSAGE_RESPONSE.toString();

export type StorageSyncRequestListener = (request: StorageSyncRequest) => void;
export type StorageSyncResponseListener = (response: StorageSyncResponse) => void;
export type SubmitMessageRequestListener = (request: SubmitMessageRequest) => void;
export type SubmitMessageResponseListener = (response: SubmitMessageResponse) => void;

export type OutboundMessageListener = (message: Message) => void;

/**
 * Process received message and dispatch based on type.
 * This is for inbound messages and the actual type of message is catched by
 * the on[MessageType]() methods.
 * For outbound messages the emit[MessageType]() methods are used
 */
export class GridDispatcher {
    private _codec = new GridCodec();
    private _emitter = new EventEmitter();
    private _dispatcher: MessageProcessor<void>;

    public constructor() {
        this._dispatcher = this._createMessageDispatcher();
    }

    public dispatchInboundMessage(message: Message): void {
        this._dispatcher.process(message);
    }

    public onOutboundMessage(listener: OutboundMessageListener): this {
        this._emitter.on(OUTBOUND_MESSAGE, listener);
        return this;
    }

    public offOutboundMessage(listener: OutboundMessageListener): this {
        this._emitter.off(OUTBOUND_MESSAGE, listener);
        return this;
    }


    public onInboundStorageSyncRequest(listener: StorageSyncRequestListener): this {
        this._emitter.on(STORAGE_SYNC_REQUEST, listener);
        return this;
    }

    public offInboundStorageSyncRequest(listener: StorageSyncRequestListener): this {
        this._emitter.off(STORAGE_SYNC_REQUEST, listener);
        return this;
    }

    public emitOutboundStorageSyncRequest(request: StorageSyncRequest) {
        try {
            const message = this._codec.encodeStorageSyncRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn(`emitStorageSyncRequest(): Error occurred while encoding`, err);
        }
    }

    public onInboundStorageSyncResponse(listener: StorageSyncResponseListener): this {
        this._emitter.on(STORAGE_SYNC_RESPONSE, listener);
        return this;
    }

    public offInboundStorageSyncResponse(listener: StorageSyncResponseListener): this {
        this._emitter.off(STORAGE_SYNC_RESPONSE, listener);
        return this;
    }

    public emitOutboundStorageSyncResponse(request: StorageSyncResponse) {
        try {
            const message = this._codec.encodeStorageSyncResponse(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn(`emitStorageSyncResponse(): Error occurred while encoding`, err);
        }
    }

    public onInboundSubmitMessageRequest(listener: SubmitMessageRequestListener): this {
        this._emitter.on(SUBMIT_MESSAGE_REQUEST, listener);
        return this;
    }

    public offInboundSubmitMessageRequest(listener: SubmitMessageRequestListener): this {
        this._emitter.off(SUBMIT_MESSAGE_REQUEST, listener);
        return this;
    }

    public emitOutboundSubmitMessageRequest(request: SubmitMessageRequest) {
        try {
            const message = this._codec.encodeSubmitMessageRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn(`emitSubmitMessageRequest(): Error occurred while encoding`, err);
        }
    }

    public onInboundSubmitMessageResponse(listener: SubmitMessageResponseListener): this {
        this._emitter.on(SUBMIT_MESSAGE_RESPONSE, listener);
        return this;
    }

    public offInboundSubmitMessageResponse(listener: SubmitMessageResponseListener): this {
        this._emitter.off(SUBMIT_MESSAGE_RESPONSE, listener);
        return this;
    }

    public emitOutboundSubmitMessageResponse(request: SubmitMessageResponse) {
        try {
            const message = this._codec.encodeSubmitMessageResponse(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn(`emitSubmitMessageResponse(): Error occurred while encoding`, err);
        }
    }

    private _emitOutboundMessage(message: Message): void {
        try {
            this._emitter.emit(OUTBOUND_MESSAGE, message);
        } catch (err) {
            logger.warn("send(): Error occurred while encoding", err);
        }
    }

    private _createMessageDispatcher(): MessageProcessor<void> {
        const codec = this._codec;
        const emitter = this._emitter;
        return new class extends MessageDefaultProcessor<void> {
            
            protected processStorageSyncRequest(message: Message): void {
                try {
                    const request = codec.decodeStorageSyncRequest(message);
                    emitter.emit(STORAGE_SYNC_REQUEST, request);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processStorageSyncResponse(message: Message): void {
                try {
                    const response = codec.decodeStorageSyncResponse(message);
                    emitter.emit(STORAGE_SYNC_RESPONSE, response);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processSubmitMessageRequest(message: Message): void {
                try {
                    const request = codec.decodeSubmitMessageRequest(message);
                    emitter.emit(SUBMIT_MESSAGE_REQUEST, request);
                } catch (err) {
                    logger.warn("processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processSubmitMessageResponse(message: Message): void {
                try {
                    const response = codec.decodeSubmitMessageResponse(message);
                    emitter.emit(SUBMIT_MESSAGE_RESPONSE, response);
                } catch (err) {
                    logger.warn("processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processMessage(message: Message): void {
                logger.debug(`processMessage(): message type ${message.type} is not dispatched`);
            }

            protected processUnrecognizedMessage(message: Message): void {
                logger.warn(`processMessage(): message type ${message.type} is not recognized`);
            }
        }
    }
}