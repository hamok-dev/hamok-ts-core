import { 
    Message,
    MessageType, 
    MessageProcessor, 
    Collections,
    createLogger,
    ClearEntriesNotification,
    ClearEntriesRequest,
    ClearEntriesResponse,
    EvictEntriesNotification,
    EvictEntriesRequest,
    EvictEntriesResponse,
    GetEntriesRequest,
    GetEntriesResponse,
    GetSizeRequest,
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
    GetSizeResponse,
} from "@hamok-dev/common"
import { PendingRequest } from "../messages/PendingRequest";
import { v4 as uuid } from "uuid";
import { HamokGrid } from "../HamokGrid";
import { PendingResponse } from "../messages/PendingResponse";
import { ResponseChunker } from "../messages/ResponseChunker";
import { EventEmitter } from "ws";
import { RemoteEndpointStateChangedListener } from "../raccoons/RemotePeers";

const logger = createLogger("StorageComlink");

const CLEAR_ENTRIES_REQUEST = MessageType.CLEAR_ENTRIES_REQUEST.toString();
const CLEAR_ENTRIES_NOTIFICATION = MessageType.CLEAR_ENTRIES_NOTIFICATION.toString();
const GET_ENTRIES_REQUEST = MessageType.GET_ENTRIES_REQUEST.toString();
const GET_SIZE_REQUEST = MessageType.GET_SIZE_REQUEST.toString();
const GET_KEYS_REQUEST = MessageType.GET_KEYS_REQUEST.toString();
const DELETE_ENTRIES_REQUEST = MessageType.DELETE_ENTRIES_REQUEST.toString();
const DELETE_ENTRIES_NOTIFICATION = MessageType.DELETE_ENTRIES_NOTIFICATION.toString();
const REMOVE_ENTRIES_REQUEST = MessageType.REMOVE_ENTRIES_REQUEST.toString();
const REMOVE_ENTRIES_NOTIFICATION = MessageType.REMOVE_ENTRIES_NOTIFICATION.toString();
const EVICT_ENTRIES_REQUEST = MessageType.EVICT_ENTRIES_REQUEST.toString();
const EVICT_ENTRIES_NOTIFICATION = MessageType.EVICT_ENTRIES_NOTIFICATION.toString();
const INSERT_ENTRIES_REQUEST = MessageType.INSERT_ENTRIES_REQUEST.toString();
const INSERT_ENTRIES_NOTIFICATION = MessageType.INSERT_ENTRIES_NOTIFICATION.toString();
const UPDATE_ENTRIES_REQUEST = MessageType.UPDATE_ENTRIES_REQUEST.toString();
const UPDATE_ENTRIES_NOTIFICATION = MessageType.UPDATE_ENTRIES_NOTIFICATION.toString();


export type ClearEntriesRequestListener = (request: ClearEntriesRequest) => void;
export type ClearEntriesNotificationListener = (notification: ClearEntriesNotification) => void;
export type GetEntriesRequestListener<K> = (response: GetEntriesRequest<K>) => void;
export type GetKeysRequestListener = (response: GetKeysRequest) => void;
export type GetSizeRequestListener = (response: GetSizeRequest) => void;
export type DeleteEntriesRequestListener<K> = (request: DeleteEntriesRequest<K>) => void;
export type DeleteEntriesNotificationListener<K> = (notification: DeleteEntriesNotification<K>) => void;
export type RemoveEntriesRequestListener<K> = (request: RemoveEntriesRequest<K>) => void;
export type RemoveEntriesNotificationListener<K> = (notification: RemoveEntriesNotification<K>) => void;
export type EvictEntriesRequestListener<K> = (request: EvictEntriesRequest<K>) => void;
export type EvictEntriesNotificationListener<K> = (notification: EvictEntriesNotification<K>) => void;
export type InsertEntriesRequestListener<K, V> = (request: InsertEntriesRequest<K, V>) => void;
export type InsertEntriesNotificationListener<K, V> = (notification: InsertEntriesNotification<K, V>) => void;
export type UpdateEntriesRequestListener<K, V> = (request: UpdateEntriesRequest<K, V>) => void;
export type UpdateEntriesNotificationListener<K, V> = (notification: UpdateEntriesNotification<K, V>) => void;

export type StorageComlinkConfig = {
    storageId: string,
    throwExceptionOnTimeout: boolean,
    requestTimeoutInMs: number,
}

export abstract class StorageComlink<K, V> {
    private _config: StorageComlinkConfig;
    private _grid: HamokGrid;
    private _receiver: MessageProcessor<void>;
    private _emitter = new EventEmitter();
    private _pendingRequests = new Map<string, PendingRequest>();
    private _pendingResponses = new Map<string, PendingResponse>();
    private _responseChunker: ResponseChunker;
    private _codec: StorageCodec<K, V>;
    public constructor(
        config: StorageComlinkConfig,
        grid: HamokGrid,
        responseChunker: ResponseChunker,
        codec: StorageCodec<K, V>,
    ) {
        this._config = config;
        this._grid = grid;
        this._responseChunker = responseChunker;
        this._receiver = this._createReceiver();
        this._codec = codec;
    }

    public onChangedLeaderId(listener: RemoteEndpointStateChangedListener): StorageComlink<K, V> {
        this._grid.onLeaderChanged(event => {
            const { leaderId } = event;
            listener(leaderId);
        })
        return this;
    }

    public onRemoteEndpointJoined(listener: RemoteEndpointStateChangedListener): StorageComlink<K, V> {
        this._grid.onRemoteEndpointJoined(listener);
        return this;
    }

    public offRemoteEndpointJoined(listener: RemoteEndpointStateChangedListener): StorageComlink<K, V> {
        this._grid.offRemoteEndpointJoined(listener);
        return this;
    }
    
    public onRemoteEndpointDetached(listener: RemoteEndpointStateChangedListener): StorageComlink<K, V> {
        this._grid.onRemoteEndpointDetached(listener);
        return this;
    }

    public offRemoteEndpointDetached(listener: RemoteEndpointStateChangedListener): StorageComlink<K, V> {
        this._grid.offRemoteEndpointDetached(listener);
        return this;
    }
    
    public onClearEntriesRequest(listener: ClearEntriesRequestListener): StorageComlink<K, V> {
        this._emitter.on(CLEAR_ENTRIES_REQUEST, listener);
        return this;
    }

    public offClearEntriesRequest(listener: ClearEntriesRequestListener): StorageComlink<K, V> {
        this._emitter.off(CLEAR_ENTRIES_REQUEST, listener);
        return this;
    }

    public onClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageComlink<K, V> {
        this._emitter.on(CLEAR_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageComlink<K, V> {
        this._emitter.off(CLEAR_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.on(GET_ENTRIES_REQUEST, listener);
        return this;
    }

    public offGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.off(GET_ENTRIES_REQUEST, listener);
        return this;
    }

    public onGetKeysRequest(listener: GetKeysRequestListener): StorageComlink<K, V> {
        this._emitter.on(GET_SIZE_REQUEST, listener);
        return this;
    }

    public offGetKeysRequest(listener: GetKeysRequestListener): StorageComlink<K, V> {
        this._emitter.off(GET_SIZE_REQUEST, listener);
        return this;
    }

    public onGetSizeRequest(listener: GetSizeRequestListener): StorageComlink<K, V> {
        this._emitter.on(GET_KEYS_REQUEST, listener);
        return this;
    }

    public offGetSizeRequest(listener: GetSizeRequestListener): StorageComlink<K, V> {
        this._emitter.off(GET_KEYS_REQUEST, listener);
        return this;
    }

    public onDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.on(DELETE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.off(DELETE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.on(DELETE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.off(DELETE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.on(REMOVE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.off(REMOVE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.on(REMOVE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.off(REMOVE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.on(EVICT_ENTRIES_REQUEST, listener);
        return this;
    }

    public offEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.off(EVICT_ENTRIES_REQUEST, listener);
        return this;
    }

    public onEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.on(EVICT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.off(EVICT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageComlink<K, V> {
        this._emitter.on(INSERT_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageComlink<K, V> {
        this._emitter.off(INSERT_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        this._emitter.on(INSERT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        this._emitter.off(INSERT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageComlink<K, V> {
        this._emitter.on(UPDATE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageComlink<K, V> {
        this._emitter.off(UPDATE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        this._emitter.on(UPDATE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        this._emitter.off(UPDATE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public async requestClearEntries(
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<void> {
        const requestId = uuid();
        const message = this._codec.encodeClearEntriesRequest(
            new ClearEntriesRequest(requestId)
        );
        await this._request(message, targetEndpointIds);
    }

    public sendClearEntriesResponse(response: ClearEntriesResponse): void {
        const message = this._codec.encodeClearEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public sendClearEntriesNotification(notification: ClearEntriesNotification): void {
        const message = this._codec.encodeClearEntriesNotification(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }

    public async requestGetEntries(
        keys: ReadonlySet<K>,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<ReadonlyMap<K, V>> {
        const requestId = uuid();
        const request = this._codec.encodeGetEntriesRequest(
            new GetEntriesRequest(
                keys,
                requestId,
            )
        );
        const result = new Map<K, V>();
        (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeGetEntriesResponse(response))
            .forEach(response => Collections.concatMaps(
                result,
                response.foundEntries
            ));
        return result;
    }

    public sendGetEntriesResponse(response: GetEntriesResponse<K, V>): void {
        const message = this._codec.encodeGetEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public async requestGetKeys(
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<ReadonlySet<K>> {
        const requestId = uuid();
        const request = this._codec.encodeGetKeysRequest(
            new GetKeysRequest(requestId,)
        );
        const result = new Set<K>();
        (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeGetKeysResponse(response))
            .forEach(response => Collections.concatSet(
                result,
                response.keys
            ));
        return result;
    }

    public sendGetKeysResponse(response: GetKeysResponse<K>): void {
        const message = this._codec.encodeGetKeysResponse(response);
        this._dispatchResponse(message);
    }

    public async requestGetSize(
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<number> {
        const requestId = uuid();
        const request = this._codec.encodeGetSizeRequest(
            new GetSizeRequest(requestId,)
        );
        return (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeGetSizeResponse(response))
            .reduce((acc, response) => acc + response.size, 0);
    }

    public sendGetSizeResponse(response: GetSizeResponse): void {
        const message = this._codec.encodeGetSizeResponse(response);
        this._dispatchResponse(message);
    }

    public async requestDeleteEntries(
        keys: ReadonlySet<K>,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<ReadonlySet<K>> {
        const requestId = uuid();
        const request = this._codec.encodeDeleteEntriesRequest(
            new DeleteEntriesRequest<K>(
                requestId,
                keys
            )
        );
        const result = new Set<K>();
        (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeGetKeysResponse(response))
            .forEach(response => Collections.concatSet(
                result,
                response.keys
            ));
        return result;
    }

    public sendDeleteEntriesResponse(response: DeleteEntriesResponse<K>): void {
        const message = this._codec.encodeDeleteEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public sendDeleteEntriesNotification(notification: DeleteEntriesNotification<K>): void {
        const message = this._codec.encodeDeleteEntriesNotification(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }

    public async requestRemoveEntries(
        keys: ReadonlySet<K>,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<ReadonlyMap<K, V>> {
        const requestId = uuid();
        const request = this._codec.encodeRemoveEntriesRequest(
            new RemoveEntriesRequest<K>(
                requestId,
                keys
            )
        );
        const result = new Map<K, V>();
        (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeRemoveEntriesResponse(response))
            .forEach(response => Collections.concatMaps(
                result,
                response.removedEntries
            ));
        return result;
    }

    public sendRemoveEntriesResponse(response: RemoveEntriesResponse<K, V>): void {
        const message = this._codec.encodeRemoveEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public sendRemoveEntriesNotification(notification: RemoveEntriesNotification<K>): void {
        const message = this._codec.encodeRemoveEntriesNotification(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }


    public async requestEvictEntries(
        keys: ReadonlySet<K>,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<void> {
        const requestId = uuid();
        const message = this._codec.encodeEvictEntriesRequest(
            new EvictEntriesRequest<K>(
                requestId,
                keys
            )
        );
        await this._request(message, targetEndpointIds);
    }

    public sendEvictEntriesResponse(response: EvictEntriesRequest<K>): void {
        const message = this._codec.encodeEvictEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public sendEvictEntriesNotification(notification: EvictEntriesNotification<K>): void {
        const message = this._codec.encodeEvictEntriesNotification(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }


    public async requestInsertEntries(
        entries: ReadonlyMap<K, V>,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<ReadonlyMap<K, V>> {
        const requestId = uuid();
        const request = this._codec.encodeInsertEntriesRequest(
            new InsertEntriesRequest<K, V>(
                requestId,
                entries
            )
        );
        const result = new Map<K, V>();
        (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeInsertEntriesResponse(response))
            .forEach(response => Collections.concatMaps(
                result,
                response.existingEntries
            ));
        return result;
    }

    public sendInsertEntriesResponse(response: InsertEntriesResponse<K, V>): void {
        const message = this._codec.encodeEvictEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public sendInsertEntriesNotification(notification: InsertEntriesNotification<K, V>): void {
        const message = this._codec.encodeInsertEntriesNotification(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }


    public async requestUpdateEntries(
        entries: ReadonlyMap<K, V>,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<ReadonlyMap<K, V>> {
        const requestId = uuid();
        const request = this._codec.encodeUpdateEntriesRequest(
            new UpdateEntriesRequest<K, V>(
                requestId,
                entries
            )
        );
        const result = new Map<K, V>();
        (await this._request(request, targetEndpointIds))
            .map(response => this._codec.decodeUpdateEntriesResponse(response))
            .forEach(response => Collections.concatMaps(
                result,
                response.updatedEntries
            ));
        return result;
    }

    public sendUpdateEntriesResponse(response: UpdateEntriesResponse<K, V>): void {
        const message = this._codec.encodeUpdateEntriesResponse(response);
        this._dispatchResponse(message);
    }

    public sendUpdateEntriesNotification(notification: UpdateEntriesNotification<K, V>): void {
        const message = this._codec.encodeUpdateEntriesNotification(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }

    private async _request(message: Message, targetEndpointIds?: ReadonlySet<string>, attempt?: number): Promise<Message[]> {
        if (!message.requestId) {
            logger.warn(`Cannot send request message without a requestId`, message);
            return Promise.resolve([]);
        }
        const destinationEndpointIds = targetEndpointIds ?? this.defaultResolvingEndpointIds();
        const pendingRequest = PendingRequest.builder()
            .withRequestId(message.requestId)
            .withPendingEndpoints(destinationEndpointIds)
            .withTimeoutInMs(this._config.requestTimeoutInMs)
            .withThrowingTimeoutException(this._config.throwExceptionOnTimeout)
            .build();
        const prevPendingRequest = this._pendingRequests.get(pendingRequest.id);
        if (!prevPendingRequest) {
            logger.warn(`Pending Request was already exists for requestId ${pendingRequest.id}`);
        }
        this._pendingRequests.set(pendingRequest.id, pendingRequest);
        this._dispatchRequest(message, destinationEndpointIds);
        const tried = attempt ?? 0;
        return pendingRequest.then(responses => {
            return responses;
        }).catch(err => {
            if (tried < 3) {
                return this._request(message, targetEndpointIds, tried + 1);
            }
            throw err;
        });
    }

    protected abstract defaultResolvingEndpointIds(): ReadonlySet<string>;
    protected abstract sendNotification(message: Message): void;
    protected abstract sendRequest(message: Message): void;
    protected abstract sendResponse(message: Message): void;

    private _dispatch(
        message: Message, 
        forward: (message: Message) => void,
        destinationEndpointIds?: ReadonlySet<string>,
    ): void {
        message.storageId = this._config.storageId;
        if (!destinationEndpointIds) {
            forward(message);
            return;
        } else if (destinationEndpointIds.size < 1) {
            logger.warn(`Empty set of destination has been provided for request`, message);
            return;
        }
        if (destinationEndpointIds.size === 1) {
            for (const destinationId of destinationEndpointIds) {
                message.destinationId = destinationId;
                forward(message);
            }
            return;
        }
        for (const destinationId of destinationEndpointIds) {
            forward(new Message({
                ...message,
                destinationId
            }));
        }
    }

    private _dispatchRequest(message: Message, targetEndpointIds?: ReadonlySet<string>): void {
        this._dispatch(
            message,
            this.sendRequest.bind(this),
            targetEndpointIds,
        );
    }

    private _dispatchResponse(message: Message, targetEndpointIds?: ReadonlySet<string>): void {
        for (const chunk of this._responseChunker.apply(message)) {
            this._dispatch(
                chunk,
                this.sendResponse.bind(this),
                targetEndpointIds
            );
        }
    }

    private _dispatchNotification(message: Message, destinationId?: string): void {
        if (!destinationId) {
            this._dispatch(
                message,
                this.sendNotification.bind(this),
            );
            return;
        }
        this._dispatch(
            message,
            this.sendNotification.bind(this),
            new Set<string>(destinationId)
        );
    }

    private _processResponse(message: Message): void {
        const chunkedResponse = message.sequence !== undefined && message.lastMessage !== undefined;
        const onlyOneChunkExists = message.sequence === 0 && message.lastMessage === true;
        if (chunkedResponse && !onlyOneChunkExists) {
            const pendingResponseId = `${message.sourceId}#${message.requestId}`;
            let pendingResponse = this._pendingResponses.get(pendingResponseId);
            if (!pendingResponse) {
                pendingResponse = new PendingResponse();
                this._pendingResponses.set(pendingResponseId, pendingResponse);
            }
            pendingResponse.accept(message);
            if (!pendingResponse.isReady) {
                return;
            }
            if (!this._pendingResponses.delete(pendingResponseId)) {
                logger.warn(`Unsuccessful deleting for pending response ${pendingResponseId}`);
            }
            const assembledResponse = pendingResponse.result;
            if (!assembledResponse) {
                logger.warn(`Undefined Assembled response, cannot make a response for request ${message.requestId}`, message);
                return;
            }
            message = assembledResponse;
        }
        if (!message.requestId) {
            logger.warn(`response message does not have a requestId`, message);
            return;
        }
        const pendingRequest = this._pendingRequests.get(message.requestId);
        if (!pendingRequest) {
            logger.warn(`Cannot find pending request for requestId ${message.requestId}`, message);
            return;
        }
        if (pendingRequest.isDone()) {
            logger.warn(`Response is received for an already done request. `, pendingRequest, message);
            return;
        }
        pendingRequest.accept(message);
    }

    private _createReceiver(): MessageProcessor<void> {
        const dispatchResponse = this._processResponse.bind(this);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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
                dispatchResponse(message);
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