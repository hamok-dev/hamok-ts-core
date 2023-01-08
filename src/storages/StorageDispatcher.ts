import { 
    Message, 
    MessageType, 
    MessageProcessor, 
    createLogger,
    ClearEntriesNotification,
    ClearEntriesRequest,
    ClearEntriesResponse,
    EvictEntriesNotification,
    EvictEntriesRequest,
    EvictEntriesResponse,
    GetEntriesRequest,
    GetEntriesResponse,
    GetKeysRequest,
    GetKeysResponse,
    InsertEntriesNotification,
    InsertEntriesRequest,
    InsertEntriesResponse,
    RemoveEntriesNotification,
    RemoveEntriesRequest,
    RemoveEntriesResponse,
    UpdateEntriesNotification,
    UpdateEntriesRequest,
    UpdateEntriesResponse,
    DeleteEntriesNotification,
    DeleteEntriesRequest,
    DeleteEntriesResponse,
    StorageCodec,
    MessageDefaultProcessor,
} from "@hamok-dev/common"
import { GetSizeRequest, GetSizeResponse } from "@hamok-dev/common/lib/messagetypes/GetSize";
import { EventEmitter } from "ws";
import { ResponseChunker, StorageComlinkResponseChunkerImpl } from "../messages/ResponseChunker";

const logger = createLogger("StorageDispatcher");

const OUTBOUND_MESSAGE = "OUTBOUND_MESSAGE";
const INBOUND_RESPONSE_MESSAGE = "INBOUND_RESPONSE_MESSAGE";
const CLEAR_ENTRIES_REQUEST = MessageType.CLEAR_ENTRIES_REQUEST.toString();
const CLEAR_ENTRIES_RESPONSE = MessageType.CLEAR_ENTRIES_RESPONSE.toString();
const CLEAR_ENTRIES_NOTIFICATION = MessageType.CLEAR_ENTRIES_NOTIFICATION.toString();

const GET_ENTRIES_REQUEST = MessageType.GET_ENTRIES_REQUEST.toString();
const GET_ENTRIES_RESPONSE = MessageType.GET_ENTRIES_RESPONSE.toString();

const GET_SIZE_REQUEST = MessageType.GET_SIZE_REQUEST.toString();
const GET_SIZE_RESPONSE = MessageType.GET_SIZE_RESPONSE.toString();

const GET_KEYS_REQUEST = MessageType.GET_KEYS_REQUEST.toString();
const GET_KEYS_RESPONSE = MessageType.GET_KEYS_RESPONSE.toString();

const DELETE_ENTRIES_REQUEST = MessageType.DELETE_ENTRIES_REQUEST.toString();
const DELETE_ENTRIES_RESPONSE = MessageType.DELETE_ENTRIES_RESPONSE.toString();
const DELETE_ENTRIES_NOTIFICATION = MessageType.DELETE_ENTRIES_NOTIFICATION.toString();

const REMOVE_ENTRIES_REQUEST = MessageType.REMOVE_ENTRIES_REQUEST.toString();
const REMOVE_ENTRIES_RESPONSE = MessageType.REMOVE_ENTRIES_RESPONSE.toString();
const REMOVE_ENTRIES_NOTIFICATION = MessageType.REMOVE_ENTRIES_NOTIFICATION.toString();

const EVICT_ENTRIES_REQUEST = MessageType.EVICT_ENTRIES_REQUEST.toString();
const EVICT_ENTRIES_RESPONSE = MessageType.EVICT_ENTRIES_RESPONSE.toString();
const EVICT_ENTRIES_NOTIFICATION = MessageType.EVICT_ENTRIES_NOTIFICATION.toString();

const INSERT_ENTRIES_REQUEST = MessageType.INSERT_ENTRIES_REQUEST.toString();
const INSERT_ENTRIES_RESPONSE = MessageType.INSERT_ENTRIES_RESPONSE.toString();
const INSERT_ENTRIES_NOTIFICATION = MessageType.INSERT_ENTRIES_NOTIFICATION.toString();

const UPDATE_ENTRIES_REQUEST = MessageType.UPDATE_ENTRIES_REQUEST.toString();
const UPDATE_ENTRIES_RESPONSE = MessageType.UPDATE_ENTRIES_RESPONSE.toString();
const UPDATE_ENTRIES_NOTIFICATION = MessageType.UPDATE_ENTRIES_NOTIFICATION.toString();

export type ResponseMessageListener = (message: Message) => void;

export type ClearEntriesRequestListener = (request: ClearEntriesRequest) => void;
export type ClearEntriesResponseListener = (response: ClearEntriesResponse) => void;
export type ClearEntriesNotificationListener = (notification: ClearEntriesNotification) => void;

export type GetEntriesRequestListener<K> = (response: GetEntriesRequest<K>) => void;
export type GetEntriesResponseListener<K, V> = (request: GetEntriesResponse<K, V>) => void;

export type GetKeysRequestListener = (response: GetKeysRequest) => void;
export type GetKeysResponseListener<K> = (response: GetKeysResponse<K>) => void;

export type GetSizeRequestListener = (response: GetSizeRequest) => void;
export type GetSizeResponseListener = (response: GetSizeResponse) => void;

export type DeleteEntriesRequestListener<K> = (request: DeleteEntriesRequest<K>) => void;
export type DeleteEntriesResponseListener<K> = (response: DeleteEntriesResponse<K>) => void;
export type DeleteEntriesNotificationListener<K> = (notification: DeleteEntriesNotification<K>) => void;

export type RemoveEntriesRequestListener<K> = (request: RemoveEntriesRequest<K>) => void;
export type RemoveEntriesResponseListener<K, V> = (response: RemoveEntriesResponse<K, V>) => void;
export type RemoveEntriesNotificationListener<K> = (notification: RemoveEntriesNotification<K>) => void;

export type EvictEntriesRequestListener<K> = (request: EvictEntriesRequest<K>) => void;
export type EvictEntriesResponseListener = (response: EvictEntriesResponse) => void;
export type EvictEntriesNotificationListener<K> = (notification: EvictEntriesNotification<K>) => void;

export type InsertEntriesRequestListener<K, V> = (request: InsertEntriesRequest<K, V>) => void;
export type InsertEntriesResponseListener<K, V> = (response: InsertEntriesResponse<K, V>) => void;
export type InsertEntriesNotificationListener<K, V> = (notification: InsertEntriesNotification<K, V>) => void;

export type UpdateEntriesRequestListener<K, V> = (request: UpdateEntriesRequest<K, V>) => void;
export type UpdateEntriesResponseListener<K, V> = (response: UpdateEntriesResponse<K, V>) => void;
export type UpdateEntriesNotificationListener<K, V> = (notification: UpdateEntriesNotification<K, V>) => void;


export type OutboundMessageListener = (message: Message) => void;

export interface StorageInboundEvents<K, V> {
    onInboundResponseMessage(listener: ResponseMessageListener): StorageInboundEvents<K, V>;
    offInboundResponseMessage(listener: ResponseMessageListener): StorageInboundEvents<K, V>;

    onInboundClearEntriesRequest(listener: ClearEntriesRequestListener): StorageInboundEvents<K, V>;
    offInboundClearEntriesRequest(listener: ClearEntriesRequestListener): StorageInboundEvents<K, V>;
    // onInboundClearEntriesResponse(listener: ClearEntriesResponseListener): StorageInboundEvents<K, V>;
    // offInboundClearEntriesResponseListener(listener: ClearEntriesResponseListener): StorageInboundEvents<K, V>;
    onInboundClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageInboundEvents<K, V>;
    offInboundClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageInboundEvents<K, V>;
    
    onInboundGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageInboundEvents<K, V>;
    offInboundGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageInboundEvents<K, V>;
    // onInboundGetEntriesResponse(listener: GetEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    // offInboundGetEntriesResponse(listener: GetEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    
    onInboundGetKeysRequest(listener: GetKeysRequestListener): StorageInboundEvents<K, V>;
    offInboundGetKeysRequest(listener: GetKeysRequestListener): StorageInboundEvents<K, V>;
    // onInboundGetKeysResponse(listener: GetKeysResponseListener<K>): StorageInboundEvents<K, V>;
    // offInboundGetKeysResponse(listener: GetKeysResponseListener<K>): StorageInboundEvents<K, V>;
    
    onInboundGetSizeRequest(listener: GetSizeRequestListener): StorageInboundEvents<K, V>;
    offInboundGetSizeRequest(listener: GetSizeRequestListener): StorageInboundEvents<K, V>;
    // onInboundGetSizeResponse(listener: GetSizeResponseListener): StorageInboundEvents<K, V>;
    // offInboundGetSizeResponse(listener: GetSizeResponseListener): StorageInboundEvents<K, V>;
    
    onInboundDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageInboundEvents<K, V>;
    offInboundDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageInboundEvents<K, V> ;
    // onInboundDeleteEntriesResponse(listener: DeleteEntriesResponseListener<K>): StorageInboundEvents<K, V>;
    // offInboundDeleteEntriesResponse(listener: DeleteEntriesResponseListener<K>): StorageInboundEvents<K, V>;
    onInboundDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageInboundEvents<K, V>;
    offInboundDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageInboundEvents<K, V>;
    
    onInboundRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageInboundEvents<K, V>;
    offInboundRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageInboundEvents<K, V> ;
    // onInboundRemoveEntriesResponse(listener: RemoveEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    // offInboundRemoveEntriesResponse(listener: RemoveEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    onInboundRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageInboundEvents<K, V>;
    offInboundRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageInboundEvents<K, V>;
    
    onInboundEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageInboundEvents<K, V>;
    offInboundEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageInboundEvents<K, V> ;
    // onInboundEvictEntriesResponse(listener: EvictEntriesResponseListener): StorageInboundEvents<K, V>;
    // offInboundEvictEntriesResponse(listener: EvictEntriesResponseListener): StorageInboundEvents<K, V>;
    onInboundEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageInboundEvents<K, V>;
    offInboundEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageInboundEvents<K, V>;
    
    onInboundInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageInboundEvents<K, V>;
    offInboundInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageInboundEvents<K, V> ;
    // onInboundInsertEntriesResponse(listener: InsertEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    // offInboundInsertEntriesResponse(listener: InsertEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    onInboundInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageInboundEvents<K, V>;
    offInboundInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageInboundEvents<K, V>;
    
    onInboundUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageInboundEvents<K, V>;
    offInboundUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageInboundEvents <K, V>;
    // onInboundUpdateEntriesResponse(listener: UpdateEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    // offInboundUpdateEntriesResponse(listener: UpdateEntriesResponseListener<K, V>): StorageInboundEvents<K, V>;
    onInboundUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageInboundEvents<K, V>;
    offInboundUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageInboundEvents<K, V>;
    
}

export interface StorageOutboundEmitter<K, V> {
    emitOutboundClearEntriesRequest(request: ClearEntriesRequest): void
    emitOutboundClearEntriesResponse(notification: ClearEntriesResponse): void;
    emitOutboundClearEntriesNotification(request: ClearEntriesNotification): void;
    
    emitOutboundGetEntriesRequest(request: GetEntriesRequest<K>): void
    emitOutboundGetEntriesResponse(response: GetEntriesResponse<K, V>): void;
  
    emitOutboundGetKeysRequest(request: GetKeysRequest): void
    emitOutboundGetKeysResponse(response: GetKeysResponse<K>): void;
  
    emitOutboundGetSizeRequest(request: GetSizeRequest): void
    emitOutboundGetSizeResponse(response: GetSizeResponse): void;
  
    emitOutboundDeleteEntriesRequest(request: DeleteEntriesRequest<K>): void
    emitOutboundDeleteEntriesResponse(notification: DeleteEntriesResponse<K>): void;
    emitOutboundDeleteEntriesNotification(request: DeleteEntriesNotification<K>): void;
   
    emitOutboundRemoveEntriesRequest(request: RemoveEntriesRequest<K>): void
    emitOutboundRemoveEntriesResponse(notification: RemoveEntriesResponse<K, V>): void;
    emitOutboundRemoveEntriesNotification(request: RemoveEntriesNotification<K>): void;
   
    emitOutboundEvictEntriesRequest(request: EvictEntriesRequest<K>): void
    emitOutboundEvictEntriesResponse(notification: EvictEntriesResponse): void;
    emitOutboundEvictEntriesNotification(request: EvictEntriesNotification<K>): void;
   
    emitOutboundInsertEntriesRequest(request: InsertEntriesRequest<K, V>): void
    emitOutboundInsertEntriesResponse(notification: InsertEntriesResponse<K, V>): void;
    emitOutboundInsertEntriesNotification(request: InsertEntriesNotification<K, V>): void;
   
    emitOutboundUpdateEntriesRequest(request: UpdateEntriesRequest<K, V>): void
    emitOutboundUpdateEntriesResponse(notification: UpdateEntriesResponse<K, V>): void;
    emitOutboundUpdateEntriesNotification(request: UpdateEntriesNotification<K, V>): void;
   
}

export class StorageDispatcher<K, V> implements StorageInboundEvents<K, V>, StorageOutboundEmitter<K, V> {
    private _codec: StorageCodec<K, V>;
    private _emitter = new EventEmitter();
    private _responseChunker: ResponseChunker
    private _dispatcher: MessageProcessor<void>;

    public constructor(
        codec: StorageCodec<K, V>,
        responseChunker?: ResponseChunker
    ) {
        this._codec = codec;
        this._responseChunker = responseChunker ?? StorageComlinkResponseChunkerImpl.createPassiveChunker();
        this._dispatcher = this._createMessageDispatcher();
    }

    public emitInboundMessage(message: Message): void {
        this._dispatcher.process(message);
    }

    public onInboundResponseMessage(listener: ResponseMessageListener): this {
        this._emitter.on(INBOUND_RESPONSE_MESSAGE, listener);
        return this;
    }

    public offInboundResponseMessage(listener: ResponseMessageListener): this {
        this._emitter.off(INBOUND_RESPONSE_MESSAGE, listener);
        return this;
    }

    public onInboundClearEntriesRequest(listener: ClearEntriesRequestListener): StorageInboundEvents<K, V> {
        this._emitter.on(CLEAR_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundClearEntriesRequest(listener: ClearEntriesRequestListener): StorageInboundEvents<K, V> {
        this._emitter.off(CLEAR_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundClearEntriesResponse(listener: ClearEntriesResponseListener): StorageInboundEvents<K, V> {
        this._emitter.on(CLEAR_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundClearEntriesResponseListener(listener: ClearEntriesResponseListener): StorageInboundEvents<K, V> {
        this._emitter.off(CLEAR_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageInboundEvents<K, V> {
        this._emitter.on(CLEAR_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInboundClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageInboundEvents<K, V> {
        this._emitter.off(CLEAR_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInboundGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(GET_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(GET_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundGetEntriesResponse(listener: GetEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(GET_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundGetEntriesResponse(listener: GetEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(GET_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundGetKeysRequest(listener: GetKeysRequestListener): StorageInboundEvents<K, V> {
        this._emitter.on(GET_SIZE_REQUEST, listener);
        return this;
    }

    public offInboundGetKeysRequest(listener: GetKeysRequestListener): StorageInboundEvents<K, V> {
        this._emitter.off(GET_SIZE_REQUEST, listener);
        return this;
    }

    public onInboundGetKeysResponse(listener: GetKeysResponseListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(GET_SIZE_RESPONSE, listener);
        return this;
    }

    public offInboundGetKeysResponse(listener: GetKeysResponseListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(GET_SIZE_RESPONSE, listener);
        return this;
    }

    public onInboundGetSizeRequest(listener: GetSizeRequestListener): StorageInboundEvents<K, V> {
        this._emitter.on(GET_KEYS_REQUEST, listener);
        return this;
    }

    public offInboundGetSizeRequest(listener: GetSizeRequestListener): StorageInboundEvents<K, V> {
        this._emitter.off(GET_KEYS_REQUEST, listener);
        return this;
    }

    public onInboundGetSizeResponse(listener: GetSizeResponseListener): StorageInboundEvents<K, V> {
        this._emitter.on(GET_KEYS_RESPONSE, listener);
        return this;
    }

    public offInboundGetSizeResponse(listener: GetSizeResponseListener): StorageInboundEvents<K, V> {
        this._emitter.off(GET_KEYS_RESPONSE, listener);
        return this;
    }

    public onInboundDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(DELETE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(DELETE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundDeleteEntriesResponse(listener: DeleteEntriesResponseListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(DELETE_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundDeleteEntriesResponse(listener: DeleteEntriesResponseListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(DELETE_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(DELETE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInboundDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(DELETE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInboundRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(REMOVE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(REMOVE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundRemoveEntriesResponse(listener: RemoveEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(REMOVE_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundRemoveEntriesResponse(listener: RemoveEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(REMOVE_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(REMOVE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInboundRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(REMOVE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInboundEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(EVICT_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(EVICT_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundEvictEntriesResponse(listener: EvictEntriesResponseListener): StorageInboundEvents<K, V> {
        this._emitter.on(EVICT_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundEvictEntriesResponse(listener: EvictEntriesResponseListener): StorageInboundEvents<K, V> {
        this._emitter.off(EVICT_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageInboundEvents<K, V> {
        this._emitter.on(EVICT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInboundEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageInboundEvents<K, V> {
        this._emitter.off(EVICT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInboundInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(INSERT_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(INSERT_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundInsertEntriesResponse(listener: InsertEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(INSERT_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundInsertEntriesResponse(listener: InsertEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(INSERT_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(INSERT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInboundInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(INSERT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInboundUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(UPDATE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInboundUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(UPDATE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInboundUpdateEntriesResponse(listener: UpdateEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(UPDATE_ENTRIES_RESPONSE, listener);
        return this;
    }

    public offInboundUpdateEntriesResponse(listener: UpdateEntriesResponseListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(UPDATE_ENTRIES_RESPONSE, listener);
        return this;
    }

    public onInboundUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.on(UPDATE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInboundUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageInboundEvents<K, V> {
        this._emitter.off(UPDATE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public emitOutboundClearEntriesRequest(request: ClearEntriesRequest): void {
        try {
            const message = this._codec.encodeClearEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundClearEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundClearEntriesResponse(response: ClearEntriesResponse): void {
        try {
            const message = this._codec.encodeClearEntriesResponse(response);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundClearEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundClearEntriesNotification(notification: ClearEntriesNotification): void {
        try {
            const message = this._codec.encodeClearEntriesNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundClearEntriesNotification(): Error occurred while encoding", err);
        }
    }

    public emitOutboundGetEntriesRequest(request: GetEntriesRequest<K>): void {
        try {
            const message = this._codec.encodeGetEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundGetEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundGetEntriesResponse(response: GetEntriesResponse<K, V>): void {
        try {
            const message = this._codec.encodeGetEntriesResponse(response);
            for (const chunk of this._responseChunker.apply(message)) {
                this._emitOutboundMessage(chunk);
            }
        } catch (err) {
            logger.warn("emitOutboundGetEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundGetKeysRequest(request: GetKeysRequest): void {
        try {
            const message = this._codec.encodeGetKeysRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundGetKeysRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundGetKeysResponse(response: GetKeysResponse<K>): void {
        try {
            const message = this._codec.encodeGetKeysResponse(response);
            for (const chunk of this._responseChunker.apply(message)) {
                this._emitOutboundMessage(chunk);
            }
        } catch (err) {
            logger.warn("emitOutboundGetKeysResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundGetSizeRequest(request: GetSizeRequest): void {
        try {
            const message = this._codec.encodeGetSizeRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundGetSizeRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundGetSizeResponse(response: GetSizeResponse): void {
        try {
            const message = this._codec.encodeGetSizeResponse(response);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundGetSizeResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundDeleteEntriesRequest(request: DeleteEntriesRequest<K>): void {
        try {
            const message = this._codec.encodeDeleteEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundDeleteEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundDeleteEntriesResponse(response: DeleteEntriesResponse<K>): void {
        try {
            const message = this._codec.encodeDeleteEntriesResponse(response);
            for (const chunk of this._responseChunker.apply(message)) {
                this._emitOutboundMessage(chunk);
            }
        } catch (err) {
            logger.warn("emitOutboundDeleteEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundDeleteEntriesNotification(notification: DeleteEntriesNotification<K>): void {
        try {
            const message = this._codec.encodeDeleteEntriesNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundDeleteEntriesNotification(): Error occurred while encoding", err);
        }
    }

    public emitOutboundRemoveEntriesRequest(request: RemoveEntriesRequest<K>): void {
        try {
            const message = this._codec.encodeRemoveEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundRemoveEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundRemoveEntriesResponse(response: RemoveEntriesResponse<K, V>): void {
        try {
            const message = this._codec.encodeRemoveEntriesResponse(response);
            for (const chunk of this._responseChunker.apply(message)) {
                this._emitOutboundMessage(chunk);
            }
        } catch (err) {
            logger.warn("emitOutboundRemoveEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundRemoveEntriesNotification(notification: RemoveEntriesNotification<K>): void {
        try {
            const message = this._codec.encodeRemoveEntriesNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundRemoveEntriesNotification(): Error occurred while encoding", err);
        }
    }

    public emitOutboundEvictEntriesRequest(request: EvictEntriesRequest<K>): void {
        try {
            const message = this._codec.encodeEvictEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundEvictEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundEvictEntriesResponse(response: EvictEntriesResponse): void {
        try {
            const message = this._codec.encodeEvictEntriesResponse(response);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundEvictEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundEvictEntriesNotification(notification: EvictEntriesNotification<K>): void {
        try {
            const message = this._codec.encodeEvictEntriesNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundEvictEntriesNotification(): Error occurred while encoding", err);
        }
    }

    public emitOutboundInsertEntriesRequest(request: InsertEntriesRequest<K, V>): void {
        try {
            const message = this._codec.encodeInsertEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundInsertEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundInsertEntriesResponse(response: InsertEntriesResponse<K, V>): void {
        try {
            const message = this._codec.encodeInsertEntriesResponse(response);
            for (const chunk of this._responseChunker.apply(message)) {
                this._emitOutboundMessage(chunk);
            }
        } catch (err) {
            logger.warn("emitOutboundInsertEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundInsertEntriesNotification(notification: InsertEntriesNotification<K, V>): void {
        try {
            const message = this._codec.encodeInsertEntriesNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundInsertEntriesNotification(): Error occurred while encoding", err);
        }
    }

    public emitOutboundUpdateEntriesRequest(request: UpdateEntriesRequest<K, V>): void {
        try {
            const message = this._codec.encodeUpdateEntriesRequest(request);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundUpdateEntriesRequest(): Error occurred while encoding", err);
        }
    }

    public emitOutboundUpdateEntriesResponse(response: UpdateEntriesResponse<K, V>): void {
        try {
            const message = this._codec.encodeUpdateEntriesResponse(response);
            for (const chunk of this._responseChunker.apply(message)) {
                this._emitOutboundMessage(chunk);
            }
        } catch (err) {
            logger.warn("emitOutboundUpdateEntriesResponse(): Error occurred while encoding", err);
        }
    }

    public emitOutboundUpdateEntriesNotification(notification: UpdateEntriesNotification<K, V>): void {
        try {
            const message = this._codec.encodeUpdateEntriesNotification(notification);
            this._emitOutboundMessage(message);
        } catch (err) {
            logger.warn("emitOutboundUpdateEntriesNotification(): Error occurred while encoding", err);
        }
    }

    private _emitOutboundMessage(message: Message) {
        try {
            this._emitter.emit(OUTBOUND_MESSAGE, message);
        } catch (err) {
            logger.warn("_emitOutboundMessage(): Error occurred while encoding", err);
        }
    }


    private _createMessageDispatcher(): MessageProcessor<void> {
        const codec = this._codec;
        const emitter = this._emitter;
        const result = new class extends MessageDefaultProcessor<void> {

            protected processClearEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeClearEntriesRequest(message);
                    emitter.emit(CLEAR_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processClearEntriesRequest(): Error occurred while decoding message", err)
                }
            }
            protected processClearEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeClearEntriesResponse(message);
                    emitter.emit(CLEAR_ENTRIES_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processClearEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processClearEntriesNotification(message: Message): void {
                try {
                    const notification = codec.decodeClearEntriesNotification(message);
                    emitter.emit(CLEAR_ENTRIES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processClearEntriesNotification(): Error occurred while decoding message", err)
                }
            }
            protected processGetEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeGetEntriesRequest(message);
                    emitter.emit(GET_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processGetEntriesRequest(): Error occurred while decoding message", err)
                }
            }
            protected processGetEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeGetEntriesResponse(message);
                    emitter.emit(GET_ENTRIES_RESPONSE, notification);
                } catch (err) {
                    logger.warn("dispatcher::processGetEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processGetSizeRequest(message: Message): void {
                try {
                    const notification = codec.decodeGetEntriesRequest(message);
                    emitter.emit(GET_SIZE_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processGetSizeRequest(): Error occurred while decoding message", err)
                }
            }
            protected processGetSizeResponse(message: Message): void {
                try {
                    const notification = codec.decodeGetSizeResponse(message);
                    emitter.emit(GET_SIZE_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processGetSizeResponse(): Error occurred while decoding message", err)
                }
            }
            protected processGetKeysRequest(message: Message): void {
                try {
                    const notification = codec.decodeGetKeysRequest(message);
                    emitter.emit(GET_KEYS_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processGetKeysRequest(): Error occurred while decoding message", err)
                }
            }
            protected processGetKeysResponse(message: Message): void {
                try {
                    const notification = codec.decodeGetKeysResponse(message);
                    emitter.emit(GET_KEYS_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processGetKeysResponse(): Error occurred while decoding message", err)
                }
            }
            protected processDeleteEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeDeleteEntriesRequest(message);
                    emitter.emit(DELETE_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processDeleteEntriesRequest(): Error occurred while decoding message", err)
                }
            }
            protected processDeleteEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeDeleteEntriesResponse(message);
                    emitter.emit(DELETE_ENTRIES_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processDeleteEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processDeleteEntriesNotification(message: Message): void {
                try {
                    const notification = codec.decodeDeleteEntriesNotification(message);
                    emitter.emit(DELETE_ENTRIES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processDeleteEntriesNotification(): Error occurred while decoding message", err)
                }
            }
            protected processRemoveEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeRemoveEntriesRequest(message);
                    emitter.emit(REMOVE_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processRemoveEntriesRequest(): Error occurred while decoding message", err)
                }
            }
            protected processRemoveEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeRemoveEntriesResponse(message);
                    emitter.emit(REMOVE_ENTRIES_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processRemoveEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processRemoveEntriesNotification(message: Message): void {
                try {
                    const notification = codec.decodeRemoveEntriesNotification(message);
                    emitter.emit(REMOVE_ENTRIES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processRemoveEntriesNotification(): Error occurred while decoding message", err)
                }
            }
            protected processEvictEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeEvictEntriesRequest(message);
                    emitter.emit(EVICT_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processEvictEntriesRequest(): Error occurred while decoding message", err)
                }
            }
            protected processEvictEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeEvictEntriesResponse(message);
                    emitter.emit(EVICT_ENTRIES_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processEvictEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processEvictEntriesNotification(message: Message): void {
                try {
                    const notification = codec.decodeEvictEntriesNotification(message);
                    emitter.emit(EVICT_ENTRIES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processEvictEntriesNotification(): Error occurred while decoding message", err)
                }
            }
            protected processInsertEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeInsertEntriesRequest(message);
                    emitter.emit(INSERT_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processInsertEntriesRequest(): Error occurred while decoding message", err)
                }
            }
            protected processInsertEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeInsertEntriesResponse(message);
                    emitter.emit(INSERT_ENTRIES_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processInsertEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processInsertEntriesNotification(message: Message): void {
                try {
                    const notification = codec.decodeInsertEntriesNotification(message);
                    emitter.emit(INSERT_ENTRIES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processInsertEntriesNotification(): Error occurred while decoding message", err)
                }
            }
            protected processUpdateEntriesRequest(message: Message): void {
                try {
                    const notification = codec.decodeUpdateEntriesRequest(message);
                    emitter.emit(UPDATE_ENTRIES_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processHelloNotification(): Error occurred while decoding message", err)
                }
            }
            protected processUpdateEntriesResponse(message: Message): void {
                try {
                    const notification = codec.decodeUpdateEntriesResponse(message);
                    emitter.emit(UPDATE_ENTRIES_RESPONSE, notification);
                    emitter.emit(INBOUND_RESPONSE_MESSAGE, message);
                } catch (err) {
                    logger.warn("dispatcher::processUpdateEntriesResponse(): Error occurred while decoding message", err)
                }
            }
            protected processUpdateEntriesNotification(message: Message): void {
                try {
                    const notification = codec.decodeUpdateEntriesNotification(message);
                    emitter.emit(UPDATE_ENTRIES_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processUpdateEntriesNotification(): Error occurred while decoding message", err)
                }
            }
            protected processMessage(message: Message): void {
                logger.debug(`processMessage(): message type ${message.type} is not dispatched`);
            }

            protected processUnrecognizedMessage(message: Message): void {
                logger.warn(`processMessage(): message type ${message.type} is not recognized`);
            }
        }
        return result;
    }
}