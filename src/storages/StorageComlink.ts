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
    OngoingRequestsNotification,
    GetSizeResponse,
} from "@hamok-dev/common"
import { PendingRequest } from "../messages/PendingRequest";
import { v4 as uuid } from "uuid";
import { HamokGrid, StorageSyncResult } from "../HamokGrid";
import { PendingResponse } from "../messages/PendingResponse";
import { ResponseChunker } from "../messages/ResponseChunker";
import { EventEmitter } from "ws";
import { RemoteEndpointStateChangedListener } from "../raccoons/RemotePeers";
import { CompletablePromise } from "../utils/CompletablePromise";
import { OngoingRequestIds } from "../messages/OngoingRequestIds";
import { MessageEmitter } from "../messages/MessageEmitter";

const logger = createLogger("StorageComlink");

const STORAGE_SYNC_REQUESTED_EVENT_NAME = "StorageSyncRequested";
const CHANGED_LEADER_ID_EVENT_NAME = "ChangedLeaderId";
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

export type ChangedLeaderIdListener = (newLeaderId?: string) => void;
export type StorageSyncRequestedListener = (request: CompletablePromise<StorageSyncResult>) => Promise<void>;
export type ClearEntriesRequestListener = (request: ClearEntriesRequest) => Promise<void>;
export type ClearEntriesNotificationListener = (notification: ClearEntriesNotification) => Promise<void>;
export type GetEntriesRequestListener<K> = (response: GetEntriesRequest<K>) => Promise<void>;
export type GetKeysRequestListener = (response: GetKeysRequest) => Promise<void>;
export type GetSizeRequestListener = (response: GetSizeRequest) => Promise<void>;
export type DeleteEntriesRequestListener<K> = (request: DeleteEntriesRequest<K>) => Promise<void>;
export type DeleteEntriesNotificationListener<K> = (notification: DeleteEntriesNotification<K>) => Promise<void>;
export type RemoveEntriesRequestListener<K> = (request: RemoveEntriesRequest<K>) => Promise<void>;
export type RemoveEntriesNotificationListener<K> = (notification: RemoveEntriesNotification<K>) => Promise<void>;
export type EvictEntriesRequestListener<K> = (request: EvictEntriesRequest<K>) => Promise<void>;
export type EvictEntriesNotificationListener<K> = (notification: EvictEntriesNotification<K>) => Promise<void>;
export type InsertEntriesRequestListener<K, V> = (request: InsertEntriesRequest<K, V>) => Promise<void>;
export type InsertEntriesNotificationListener<K, V> = (notification: InsertEntriesNotification<K, V>) => Promise<void>;
export type UpdateEntriesRequestListener<K, V> = (request: UpdateEntriesRequest<K, V>) => Promise<void>;
export type UpdateEntriesNotificationListener<K, V> = (notification: UpdateEntriesNotification<K, V>) => Promise<void>;

export type StorageComlinkSyncSettings = {
    storageSync: boolean,
    clearEntries: boolean,
    getEntries: boolean,
    getKeys: boolean,
    getSize: boolean,
    deleteEntries: boolean,
    removeEntries: boolean,
    evictEntries: boolean,
    insertEntries: boolean,
    updateEntries: boolean
}

export type StorageComlinkConfig = {
    /**
     * The identifier of the storage the comlink belongs to.
     * In case of a storage builder this infromation is automatically fetched 
     * from the given storage.
     */
    storageId: string,
    /**
     * indicate if in case of request is timed out should the comlink throws exception
     * or just assemble the response based on the info it got
     */
    throwExceptionOnTimeout: boolean,
    /**
     * Determining the timeout for a request generated by this comlink.
     * in case of a storage builder belongs to a hamok grid, the default value is 
     * the grid request timeout config setting
     */
    requestTimeoutInMs: number,
    /**
     * Determine how many response is necessary to resolve the request. 
     */
    neededResponse: number,
    /**
     * In case a requestId is added explicitly to the ongoing requests set
     * by calling the addOngoingRequestId() this setting determines the period 
     * to send notification to the source(s) about an ongoing requests to prevent timeout there
     * The notification stopped sent if removeOngoingRequestId is called, and it is very important 
     * to call it explicitly in any case. In another word comlink is not responsible to handle 
     * automatically an explicitly postponed request to stop sending notification about.
     */
    ongoingRequestsSendingPeriodInMs: number;

    /**
     * Synchronization setting refer to configurations 
     * influence the message processing. By default each message received and 
     * dispatched paralelly, however 
     * that can lead to an undesired behaviour in some cases.
     * 
     * For example, let's say two commits are dispatched by a HamokGrid for the same 
     * storage and one is for delete and one is for insert. if the two commits 
     * are dispatched at the same time and delete executes faster, but the commit index 
     * was specifically the insert for first, then it's a problem.
     * To avoid such situation the synchronize settings determine which operations
     * must be executed in a blocking mode and which can run parallely.
     * 
     * Note: that storage builders may provision this settings 
     * differently and only change it if you know what you are doing.
     * 
     * Note 2: While requests are queued the comlink automatically send the ongoing request 
     * notification to the source to avoid request timeout exception
     */
    synchronize: StorageComlinkSyncSettings
}

export interface StorageGridLink {
    receive(message: Message): void;
    requestStorageSync(): Promise<StorageSyncResult>;
}


export type StorageEncoder<T> = (input: T) => Uint8Array;
export type StorageDecoder<T> = (input: Uint8Array) => T;

/**
 * Add: A wrapper for pendingRequests which takes the message
 * commitIndex into consideration when resolves the requests
 * 
 * BlockingEmitter blocks an event emission until the prev is done
 * blocking should be only for mutating requests / responses
 * ordinary get requests should go without the blocking behaviour
 */

export abstract class StorageComlink<K, V> implements StorageGridLink {
    private _config: StorageComlinkConfig;
    private _grid: HamokGrid;
    private _receiver: MessageProcessor<void>;
    private _pendingRequests = new Map<string, PendingRequest>();
    private _pendingResponses = new Map<string, PendingResponse>();
    private _responseChunker: ResponseChunker;
    private _ongoingRequestIds: OngoingRequestIds;
    private _codec: StorageCodec<K, V>;
    // private _emitter = new EventEmitter();
    private _emitter = new MessageEmitter();

    public constructor(
        config: StorageComlinkConfig,
        grid: HamokGrid,
        responseChunker: ResponseChunker,
        codec: StorageCodec<K, V>,
    ) {
        this._codec = codec;
        this._config = config;
        this._grid = grid;
        this._responseChunker = responseChunker;
        this._receiver = this._createReceiver();

        this._grid.onLeaderChanged(event => {
            const { actualLeaderId } = event;
            this._emitter.emit(CHANGED_LEADER_ID_EVENT_NAME, actualLeaderId);
        }).onRemoteEndpointDetached(remoteEndpointId => {
            for (const pendingRequest of this._pendingRequests.values()) {
                pendingRequest.removeEndpointId(remoteEndpointId);
            }
            for (const [key, pendingResponse] of Array.from(this._pendingResponses)) {
                if (pendingResponse.sourceEndpointId === remoteEndpointId) {
                    this._pendingResponses.delete(key);
                }
            }
        });
        this._ongoingRequestIds = new OngoingRequestIds(config.ongoingRequestsSendingPeriodInMs);
        this._ongoingRequestIds.sender = notification => {
            const message = this._codec.encodeOngoingRequestsNotification(notification);
            this._dispatchNotification(message, notification.destinationEndpointId)
        };
        this._emitter.onEnqueuedRequest((requestId, sourceEndpointId) => {
            this.addOngoingRequestId(requestId, sourceEndpointId);
        }).onDequeuedRequest((requestId) => {
            this.removeOngoingRequestId(requestId)
        })
    }

    public get localEndpointId(): string {
        return this._grid.localEndpointId;
    }

    public get remoteEndpointIds(): ReadonlySet<string> {
        return this._grid.remoteEndpointIds;
    }

    public receive(message: Message) {
        this._receiver.process(message);
    }

    public addOngoingRequestId(requestId: string, remoteEndpointId?: string): void {
        if (this._config.ongoingRequestsSendingPeriodInMs < 1) {
            return;
        }
        if (!remoteEndpointId) {
            logger.warn(`Cannot send pospone notification about a requestId ${requestId} becasue the remote endpointId to send is undefined`);
            return;
        }
        this._ongoingRequestIds.addOngoingRequestId(requestId, remoteEndpointId);
    }

    public removeOngoingRequestId(requestId: string): void {
        if (this._config.ongoingRequestsSendingPeriodInMs < 1) {
            return;
        }
        this._ongoingRequestIds.removeOngoingRequestId(requestId);
    }

    public async requestStorageSync(): Promise<StorageSyncResult> {
        const promise = new CompletablePromise<StorageSyncResult>();
        this._emitter.emit(STORAGE_SYNC_REQUESTED_EVENT_NAME, promise);
        return promise;
    }

    public onStorageSyncRequested(listener: StorageSyncRequestedListener): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.storageSync);
        onEvent(STORAGE_SYNC_REQUESTED_EVENT_NAME, listener);
        return this;
    }

    public offStorageSyncRequested(listener: StorageSyncRequestedListener): StorageComlink<K, V> {
        this._emitter.removeListener(STORAGE_SYNC_REQUESTED_EVENT_NAME, listener);
        return this;
    }

    public onChangedLeaderId(listener: ChangedLeaderIdListener): StorageComlink<K, V> {
        this._emitter.addListener(CHANGED_LEADER_ID_EVENT_NAME, listener);
        return this;
    }

    public offChangedLeaderId(listener: ChangedLeaderIdListener): StorageComlink<K, V> {
        this._emitter.removeListener(CHANGED_LEADER_ID_EVENT_NAME, listener);
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
        const onEvent = this._getOnEvent(this._config.synchronize.clearEntries);
        onEvent(CLEAR_ENTRIES_REQUEST, listener);
        return this;
    }

    public offClearEntriesRequest(listener: ClearEntriesRequestListener): StorageComlink<K, V> {
        this._emitter.removeListener(CLEAR_ENTRIES_REQUEST, listener);
        return this;
    }

    public onClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.clearEntries);
        onEvent(CLEAR_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offClearEntriesNotification(listener: ClearEntriesNotificationListener): StorageComlink<K, V> {
        this._emitter.removeListener(CLEAR_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.getEntries);
        onEvent(GET_ENTRIES_REQUEST, listener);
        return this;
    }

    public offGetEntriesRequest(listener: GetEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(GET_ENTRIES_REQUEST, listener);
        return this;
    }

    public onGetKeysRequest(listener: GetKeysRequestListener): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.getKeys);
        onEvent(GET_SIZE_REQUEST, listener);
        return this;
    }

    public offGetKeysRequest(listener: GetKeysRequestListener): StorageComlink<K, V> {
        this._emitter.removeListener(GET_SIZE_REQUEST, listener);
        return this;
    }

    public onGetSizeRequest(listener: GetSizeRequestListener): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.getSize);
        onEvent(GET_KEYS_REQUEST, listener);
        return this;
    }

    public offGetSizeRequest(listener: GetSizeRequestListener): StorageComlink<K, V> {
        this._emitter.removeListener(GET_KEYS_REQUEST, listener);
        return this;
    }

    public onDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.deleteEntries);
        onEvent(DELETE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offDeleteEntriesRequest(listener: DeleteEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(DELETE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.deleteEntries);
        onEvent(DELETE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offDeleteEntriesNotification(listener: DeleteEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(DELETE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.removeEntries);
        onEvent(REMOVE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offRemoveEntriesRequest(listener: RemoveEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(REMOVE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.removeEntries);
        onEvent(REMOVE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offRemoveEntriesNotification(listener: RemoveEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(REMOVE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.evictEntries);
        onEvent(EVICT_ENTRIES_REQUEST, listener);
        return this;
    }

    public offEvictEntriesRequest(listener: EvictEntriesRequestListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(EVICT_ENTRIES_REQUEST, listener);
        return this;
    }

    public onEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.evictEntries);
        onEvent(EVICT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offEvictEntriesNotification(listener: EvictEntriesNotificationListener<K>): StorageComlink<K, V> {
        this._emitter.removeListener(EVICT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.insertEntries);
        onEvent(INSERT_ENTRIES_REQUEST, listener);
        return this;
    }

    public offInsertEntriesRequest(listener: InsertEntriesRequestListener<K, V>): StorageComlink<K, V> {
        this._emitter.removeListener(INSERT_ENTRIES_REQUEST, listener);
        return this;
    }

    public onInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.insertEntries);
        onEvent(INSERT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offInsertEntriesNotification(listener: InsertEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        this._emitter.removeListener(INSERT_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public onUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.updateEntries);
        onEvent(UPDATE_ENTRIES_REQUEST, listener);
        return this;
    }

    public offUpdateEntriesRequest(listener: UpdateEntriesRequestListener<K, V>): StorageComlink<K, V> {
        this._emitter.removeListener(UPDATE_ENTRIES_REQUEST, listener);
        return this;
    }

    public onUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        const onEvent = this._getOnEvent(this._config.synchronize.updateEntries);
        onEvent(UPDATE_ENTRIES_NOTIFICATION, listener);
        return this;
    }

    public offUpdateEntriesNotification(listener: UpdateEntriesNotificationListener<K, V>): StorageComlink<K, V> {
        this._emitter.removeListener(UPDATE_ENTRIES_NOTIFICATION, listener);
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
            .map(response => this._codec.decodeDeleteEntriesResponse(response))
            .forEach(response => Collections.concatSet(
                result,
                response.deletedKeys
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

    public sendEvictEntriesResponse(response: EvictEntriesResponse): void {
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
            .withNeededResponse(this._config.neededResponse)
            .build();
        const prevPendingRequest = this._pendingRequests.get(pendingRequest.id);
        if (prevPendingRequest) {
            logger.warn(`Pending Request was already exists for requestId ${pendingRequest.id}`);
        }
        this._pendingRequests.set(pendingRequest.id, pendingRequest);
        this._dispatchRequest(message, destinationEndpointIds);
        const tried = attempt ?? 0;
        return pendingRequest.then(responses => {
            this._purgeResponseForRequest(message.requestId!);
            this._pendingRequests.delete(pendingRequest.id);
            return responses;
        }).catch(err => {
            logger.warn(`Error occurred while waiting for request ${pendingRequest}, messageType: ${message.type}. Tried: ${tried}`, err);
            this._purgeResponseForRequest(message.requestId!);
            this._pendingRequests.delete(pendingRequest.id);
            if (tried < 3) {
                return this._request(message, targetEndpointIds, tried + 1);
            }
            throw err;
        });
    }

    private _purgeResponseForRequest(requestId: string) {
        const pendingResponseKeys: string[] = [];
        for (const [key, pendingResponse] of this._pendingResponses) {
            if (pendingResponse.requestId === requestId) {
                pendingResponseKeys.push(key);
            }
        }
        pendingResponseKeys.forEach(pendingResponseKey => this._pendingResponses.delete(pendingResponseKey));
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
            // logger.info("Sending response message", message.type);
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
        if (destinationId === this.localEndpointId || message.destinationId === this.localEndpointId) {
            // loopback notifications
            this.receive(message);
            return;
        }
        this._dispatch(
            message,
            this.sendNotification.bind(this),
            new Set<string>([destinationId])
        );
    }

    private _processResponse(message: Message): void {
        if (!message.requestId || !message.sourceId) {
            logger.warn(`_processResponse(): Message does not have a requestId or sourceId`, message);
            return;
        }
        const chunkedResponse = message.sequence !== undefined && message.lastMessage !== undefined;
        const onlyOneChunkExists = message.sequence === 0 && message.lastMessage === true;
        // console.warn("_responseReceived ", message);
        if (chunkedResponse && !onlyOneChunkExists) {
            const pendingResponseId = `${message.sourceId}#${message.requestId}`;
            let pendingResponse = this._pendingResponses.get(pendingResponseId);
            if (!pendingResponse) {
                pendingResponse = new PendingResponse(message.sourceId, message.requestId);
                this._pendingResponses.set(pendingResponseId, pendingResponse);
            }
            pendingResponse.accept(message);
            if (!pendingResponse.isReady) {
                const pendingRequest = this._pendingRequests.get(message.requestId ?? "notExists");
                // let's postpone the timeout if we knoe responses are coming
                if (pendingRequest) {
                    pendingRequest.postponeTimeout();
                }
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

    private _postponePendingRequest(requestId: string): boolean {
        const pendingRequest = this._pendingRequests.get(requestId);
        if (!pendingRequest) return false;
        pendingRequest.postponeTimeout();
        return true;
    }

    private _createReceiver(): MessageProcessor<void> {
        const dispatchResponse = this._processResponse.bind(this);
        const postponePendingRequest = this._postponePendingRequest.bind(this);
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
                    const request = codec.decodeGetEntriesRequest(message);
                    emitter.emit(GET_ENTRIES_REQUEST, request);
                } catch (err) {
                    logger.warn("dispatcher::processGetEntriesRequest(): Error occurred while decoding message", err)
                }
            }

            protected processOngoingRequestsNotification(message: Message): void {
                const notification = codec.decodeOngoingRequestsNotification(message);
                for (const requestId of notification.requestIds) {
                    postponePendingRequest(requestId);
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
                    logger.warn("dispatcher::processUpdateEntriesRequest(): Error occurred while decoding message", message, err)
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
                logger.warn(`processMessage(): message type ${message.type} is not dispatched`);
            }

            protected processUnrecognizedMessage(message: Message): void {
                logger.warn(`processMessage(): message type ${message.type} is not recognized`);
            }
        }
        return result;
    }

    private _getOnEvent(blockingListener: boolean) {
        if (blockingListener) return this._emitter.addBlockingListener.bind(this._emitter);
        else return this._emitter.addListener.bind(this._emitter);
    }
}