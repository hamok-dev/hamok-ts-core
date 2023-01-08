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
import { CompletablePromise } from "./utils/CompletablePromise";

const logger = createLogger("GridTransport");

const STORAGE_SYNC_REQUEST = MessageType.STORAGE_SYNC_REQUEST.toString();
const SUBMIT_MESSAGE_REQUEST = MessageType.SUBMIT_MESSAGE_REQUEST.toString();

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
export abstract class GridDispatcher {
    private _codec = new GridCodec();
    private _storageSyncRequests = new Map<string, CompletablePromise<StorageSyncResponse>>();
    private _submitRequests = new Map<string, CompletablePromise<SubmitMessageResponse>>();
    private _emitter = new EventEmitter();
    private _receiver: MessageProcessor<void>;

    public constructor() {
        this._receiver = this._createReceiver();
    }

    protected abstract send(message: Message): void;

    public receive(message: Message): void {
        this._receiver.process(message);
    }

    public onStorageSyncRequest(listener: StorageSyncRequestListener): this {
        this._emitter.on(STORAGE_SYNC_REQUEST, listener);
        return this;
    }

    public offStorageSyncRequest(listener: StorageSyncRequestListener): this {
        this._emitter.off(STORAGE_SYNC_REQUEST, listener);
        return this;
    }

    public onSubmitMessageRequest(listener: SubmitMessageRequestListener): this {
        this._emitter.on(SUBMIT_MESSAGE_REQUEST, listener);
        return this;
    }

    public offSubmitMessageRequest(listener: SubmitMessageRequestListener): this {
        this._emitter.off(SUBMIT_MESSAGE_REQUEST, listener);
        return this;
    }

    public async requestStorageSync(request: StorageSyncRequest): Promise<StorageSyncResponse> {
        const promise = new CompletablePromise<StorageSyncResponse>();
        const message = this._codec.encodeStorageSyncRequest(request);
        this._storageSyncRequests.set(request.requestId, promise);
        this.send(message);
        return promise;
    }

    public sendStorageSyncResponse(request: StorageSyncResponse) {
        try {
            const message = this._codec.encodeStorageSyncResponse(request);
            this.send(message);
        } catch (err) {
            logger.warn(`emitStorageSyncResponse(): Error occurred while encoding`, err);
        }
    }

    public async requestSubmitMessage(request: SubmitMessageRequest): Promise<SubmitMessageResponse> {
        const promise = new CompletablePromise<SubmitMessageResponse>();
        const message = this._codec.encodeSubmitMessageRequest(request);
        this._submitRequests.set(request.requestId, promise);
        this.send(message);
        return promise;
    }

    public sendSubmitMessageResponse(request: SubmitMessageResponse) {
        try {
            const message = this._codec.encodeSubmitMessageResponse(request);
            this.send(message);
        } catch (err) {
            logger.warn(`emitSubmitMessageResponse(): Error occurred while encoding`, err);
        }
    }

    private _createReceiver(): MessageProcessor<void> {
        const codec = this._codec;
        const emitter = this._emitter;
        const storageSyncRequests = this._storageSyncRequests;
        const submitMessageRequests = this._submitRequests;
        return new class extends MessageDefaultProcessor<void> {
            
            protected processStorageSyncRequest(message: Message): void {
                try {
                    const request = codec.decodeStorageSyncRequest(message);
                    emitter.emit(STORAGE_SYNC_REQUEST, request);
                } catch (err) {
                    logger.warn("processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processStorageSyncResponse(message: Message): void {
                try {
                    const response = codec.decodeStorageSyncResponse(message);
                    const promise = storageSyncRequests.get(response.requestId);
                    if (!promise) {
                        logger.warn(`No Pending Storage Sync found for response`, response);
                        return;
                    }
                    promise.resolve(response);
                } catch (err) {
                    logger.warn("processEndpointStatesNotification(): Error occurred while decoding message", err)
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
                    const promise = submitMessageRequests.get(response.requestId);
                    if (!promise) {
                        logger.warn(`No Pending Submit Message Sync found for response`, response);
                        return;
                    }
                    promise.resolve(response);
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