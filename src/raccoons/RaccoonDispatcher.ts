import { 
    Message, 
    MessageType, 
    MessageDefaultProcessor, 
    MessageProcessor, 
    RaccoonCodec, 
    createLogger,
    HelloNotification,
    EndpointStatesNotification,
    RaftVoteRequest,
    RaftVoteResponse,
    RaftAppendEntriesRequestChunk,
    RaftAppendEntriesResponse,
} from "@hamok-dev/common"
import { EventEmitter } from "events";

const logger = createLogger("RaccoonDispatcher");

const OUTBOUND_MESSAGE = "OUTBOUND_MESSAGE";
const HELLO_NOTIFICATION = MessageType.HELLO_NOTIFICATION.toString();
const ENDPOINT_STATES_NOTIFICATION = MessageType.ENDPOINT_STATES_NOTIFICATION.toString();
const RAFT_VOTE_REQUEST = MessageType.RAFT_VOTE_REQUEST.toString();
const RAFT_VOTE_RESPONSE = MessageType.RAFT_VOTE_RESPONSE.toString();
const RAFT_APPEND_ENTRIES_REQUEST_CHUNK = MessageType.RAFT_APPEND_ENTRIES_REQUEST_CHUNK.toString();
const RAFT_APPEND_ENTRIES_RESPONSE = MessageType.RAFT_APPEND_ENTRIES_RESPONSE.toString();

export type HelloNotificationListener = (notification: HelloNotification) => void;
export type EndpointStatesNotificationListener = (notification: EndpointStatesNotification) => void;
export type RaftVoteRequestListener = (request: RaftVoteRequest) => void;
export type RaftVoteResponseListener = (response: RaftVoteResponse) => void;
export type RaftAppendRequestChunkListener = (request: RaftAppendEntriesRequestChunk) => void;
export type RaftAppendResponseListener = (response: RaftAppendEntriesResponse) => void;

export type OutboundMessageListener = (message: Message) => void;

export interface RaccoonInboundEvents {
    onInboundHelloNotification(listener: HelloNotificationListener): RaccoonInboundEvents;
    offInboundHelloNotification(listener: HelloNotificationListener): RaccoonInboundEvents ;
    onInboundEndpointStatesNotification(listener: EndpointStatesNotificationListener): RaccoonInboundEvents;
    offInboundEndpointStatesNotification(listener: EndpointStatesNotificationListener): RaccoonInboundEvents;
    onInboundRaftVoteRequest(listener: RaftVoteRequestListener): RaccoonInboundEvents;
    offInboundRaftVoteRequest(listener: RaftVoteRequestListener): RaccoonInboundEvents;
    onInboundRaftVoteResponse(listener: RaftVoteResponseListener): RaccoonInboundEvents;
    offInboundRaftVoteResponse(listener: RaftVoteResponseListener): RaccoonInboundEvents;
    onInboundRaftAppendEntriesRequestChunk(listener: RaftAppendRequestChunkListener): RaccoonInboundEvents;
    offInboundRaftAppendEntriesRequestChunk(listener: RaftAppendRequestChunkListener): RaccoonInboundEvents;
    onInboundRaftAppendEntriesResponse(listener: RaftAppendResponseListener): RaccoonInboundEvents;
    offInboundRaftAppendEntriesResponse(listener: RaftAppendResponseListener): RaccoonInboundEvents;
}

export interface RaccoonOutboundEmitter {
    emitOutboundHelloNotification(notification: HelloNotification): void
    emitOutboundEndpointStatesNotification(notification: EndpointStatesNotification): void;
    emitOutboundRaftVoteRequest(request: RaftVoteRequest): void;
    emitOutboundRaftVoteResponse(response: RaftVoteResponse): void;
    emitOutboundRaftAppendEntriesResponse(response: RaftAppendEntriesResponse): void;
    emitOutboundRaftAppendEntriesRequestChunk(request: RaftAppendEntriesRequestChunk): void;
  
}

export class RaccoonDispatcher implements RaccoonInboundEvents, RaccoonOutboundEmitter {
    private _codec = new RaccoonCodec();
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


    private _emitOutboundMessage(message: Message) {
        try {
            this._emitter.emit(OUTBOUND_MESSAGE, message);
        } catch (err) {
            logger.warn("_emitOutboundMessage(): Error occurred while encoding", err);
        }
    }

    public onInboundHelloNotification(listener: HelloNotificationListener): this {
        this._emitter.on(HELLO_NOTIFICATION, listener);
        return this;
    }

    public offInboundHelloNotification(listener: HelloNotificationListener): this {
        this._emitter.off(HELLO_NOTIFICATION, listener);
        return this;
    }

    public emitOutboundHelloNotification(notification: HelloNotification) {
        try {
            const message = this._codec.encodeHelloNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundHelloNotification(): Error occurred while encoding", err);
        }
    }

    public onInboundEndpointStatesNotification(listener: EndpointStatesNotificationListener): this {
        this._emitter.on(ENDPOINT_STATES_NOTIFICATION, listener);
        return this;
    }

    public offInboundEndpointStatesNotification(listener: EndpointStatesNotificationListener): this {
        this._emitter.off(ENDPOINT_STATES_NOTIFICATION, listener);
        return this;
    }

    public emitOutboundEndpointStatesNotification(notification: EndpointStatesNotification) {
        try {
            const message = this._codec.encodeEndpointStateNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundEndpointStatesNotification(): Error occurred while encoding", err);
        }
    }

    public onInboundRaftVoteRequest(listener: RaftVoteRequestListener): this {
        this._emitter.on(RAFT_VOTE_REQUEST, listener);
        return this;
    }

    public offInboundRaftVoteRequest(listener: RaftVoteRequestListener): this {
        this._emitter.off(RAFT_VOTE_REQUEST, listener);
        return this;
    }

    public emitOutboundRaftVoteRequest(request: RaftVoteRequest) {
        try {
            const message = this._codec.encodeRaftVoteRequest(request);
            // logger.warn(`emitOutboundRaftVoteRequest(): `, message);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundRaftVoteRequest(): Error occurred while encoding", err);
        }
    }

    public onInboundRaftVoteResponse(listener: RaftVoteResponseListener): this {
        this._emitter.on(RAFT_VOTE_RESPONSE, listener);
        return this;
    }

    public offInboundRaftVoteResponse(listener: RaftVoteResponseListener): this {
        this._emitter.off(RAFT_VOTE_RESPONSE, listener);
        return this;
    }

    public emitOutboundRaftVoteResponse(response: RaftVoteResponse) {
        try {
            const message = this._codec.encodeRaftVoteResponse(response);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundRaftVoteResponse(): Error occurred while encoding", err);
        }
    }

    public onInboundRaftAppendEntriesRequestChunk(listener: RaftAppendRequestChunkListener): this {
        this._emitter.on(RAFT_APPEND_ENTRIES_REQUEST_CHUNK, listener);
        return this;
    }

    public offInboundRaftAppendEntriesRequestChunk(listener: RaftAppendRequestChunkListener): this {
        this._emitter.off(RAFT_APPEND_ENTRIES_REQUEST_CHUNK, listener);
        return this;
    }

    public emitOutboundRaftAppendEntriesRequestChunk(request: RaftAppendEntriesRequestChunk) {
        try {
            const message = this._codec.encodeRaftAppendEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundRaftAppendEntriesRequestChunk(): Error occurred while encoding", err);
        }
    }

    public onInboundRaftAppendEntriesResponse(listener: RaftAppendResponseListener): this {
        this._emitter.on(RAFT_APPEND_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundRaftAppendEntriesResponse(listener: RaftAppendResponseListener): this {
        this._emitter.off(RAFT_APPEND_ENTRIES_RESPONSE, listener);
        return this;
    }

    public emitOutboundRaftAppendEntriesResponse(response: RaftAppendEntriesResponse) {
        try {
            const message = this._codec.encodeRaftAppendEntriesResponse(response);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundRaftAppendEntriesResponse(): Error occurred while encoding", err);
        }
    }

    private _createMessageDispatcher(): MessageProcessor<void> {
        const codec = this._codec;
        const emitter = this._emitter;
        return new class extends MessageDefaultProcessor<void> {
            
            protected processHelloNotification(message: Message): void {
                try {
                    const notification = codec.decodeHelloNotification(message);
                    emitter.emit(HELLO_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processHelloNotification(): Error occurred while decoding message", err)
                }
            }

            protected processEndpointStatesNotification(message: Message): void {
                try {
                    const notification = codec.decodeEndpointStateNotification(message);
                    emitter.emit(ENDPOINT_STATES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processRaftVoteRequest(message: Message): void {
                try {
                    const request = codec.decodeRaftVoteRequest(message);
                    emitter.emit(RAFT_VOTE_REQUEST, request);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processRaftVoteResponse(message: Message): void {
                try {
                    const response = codec.decodeRaftVoteResponse(message);
                    emitter.emit(RAFT_VOTE_RESPONSE, response);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processRaftAppendChunkRequest(message: Message): void {
                try {
                    const request = codec.decodeRaftAppendEntriesRequest(message);
                    emitter.emit(RAFT_APPEND_ENTRIES_REQUEST_CHUNK, request);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
                }
            }

            protected processRaftAppendResponse(message: Message): void {
                try {
                    const response = codec.decodeRaftAppendEntriesResponse(message);
                    emitter.emit(RAFT_APPEND_ENTRIES_RESPONSE, response);
                } catch (err) {
                    logger.warn("dispatcher::processEndpointStatesNotification(): Error occurred while decoding message", err)
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