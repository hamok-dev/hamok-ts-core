import { createLogger } from "@hamok-dev/common";
import { Storage } from "../storages/Storage";
import { StorageComlink } from "../storages/StorageComlink";
import { StorageEvents, StorageEventsImpl } from "../storages/StorageEvents";

const logger = createLogger("ReplicatedStorage");

const logNotUsedAction = (context: string, obj: any) => {
    logger.warn(
        `${context}: Incoming message has not been processed, becasue the handler is not implemented`, 
        obj
    );
}

/**
 * Replicated storage replicates all entries on all distributed storages
 */
export class ReplicatedStorage<K, V> implements Storage<K, V> {
    private _comlink: StorageComlink<K, V>;
    private constructor(
        comlink: StorageComlink<K, V>
    ) {
        this._comlink = comlink
            .onClearEntriesRequest(request => {
                const response = request.createResponse();
                this._storage.clear();
                this._comlink.sendClearEntriesResponse(response);
            })
            .onClearEntriesNotification(notification => {
                this._storage.clear();
            })
            .onGetEntriesRequest(async request => {
                const foundEntries = await this._storage.getAll(request.keys);
                const response = request.createResponse(foundEntries);
                this._comlink.sendGetEntriesResponse(response);
            })
            .onGetSizeRequest(async request => {
                const response = request.createResponse(
                    await this._storage.size()
                );
                this._comlink.sendGetSizeResponse(response);
            })
            .onGetKeysRequest(async request => {
                const response = request.createResponse(
                    await this._storage.keys()
                );
                this._comlink.sendGetKeysResponse(response);
            })
            .onDeleteEntriesRequest(async request => {
                const deletedKeys = await this._storage.deleteAll(request.keys)
                const response = request.createResponse(
                    deletedKeys
                );
                this._comlink.sendDeleteEntriesResponse(response);
            })
            .onDeleteEntriesNotification(async notification => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
                // await this._storage.deleteAll(notification.keys);
            })
            .onRemoveEntriesRequest(async request => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onRemoveEntriesNotification(async notification => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onEvictEntriesRequest(async request => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onEvictEntriesNotification(async notification => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onInsertEntriesRequest(async request => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onInsertEntriesNotification(async notification => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onUpdateEntriesRequest(async request => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onUpdateEntriesNotification(notification => {
                logNotUsedAction(
                    "onClearEntriesRequest()",
                    response,
                );
            })
            .onRemoteEndpointJoined(remoteEndpointId => {

            })
            .onRemoteEndpointDetached(remoteEndpointId => {

            })
            .onChangedLeaderId(leaderId => {

            })
            ;
    }

    public get id(): string {
        throw new Error(`Not Implemented`);
    }
    
    public get events(): StorageEvents<K, V> {
        return this._events;
    }

    public async size(): Promise<number> {
        throw new Error(`Not Implemented`);
    }

    public async isEmpty(): Promise<boolean> {
        throw new Error(`Not Implemented`);
    }

    public async keys(): Promise<ReadonlySet<K>> {
        throw new Error("Method not implemented.");
    }

    public async clear(): Promise<void> {
        throw new Error("Method not implemented.");
    }

    public async get(key: K): Promise<V | undefined> {
        throw new Error("Method not implemented.");
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        throw new Error("Method not implemented.");
    }
    
    public async set(key: K, value: V): Promise<V | undefined> {
        throw new Error("Method not implemented.");
    }
    
    public async setAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        throw new Error("Method not implemented.");
    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        throw new Error("Method not implemented.");
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        throw new Error("Method not implemented.");
    }
    
    public async delete(key: K): Promise<boolean> {
        throw new Error("Method not implemented.");
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        throw new Error("Method not implemented.");
    }
    
    public async evict(key: K): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async restore(key: K, value: V): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<[K, V]> {
        throw new Error("Method not implemented.");
    }
}