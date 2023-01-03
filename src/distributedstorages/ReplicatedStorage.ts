import { Storage } from "../storages/Storage";
import { StorageComlink } from "../storages/StorageComlink";
import { StorageEvents, StorageEventsImpl } from "../storages/StorageEvents";

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
                
            })
            .onClearEntriesNotification(notification => {

            })
            .onGetEntriesRequest(request => {

            })
            .onGetSizeRequest(request => {

            })
            .onGetKeysRequest(request => {

            })
            .onDeleteEntriesRequest(request => {

            })
            .onDeleteEntriesNotification(notification => {

            })
            .onRemoveEntriesRequest(request => {

            })
            .onRemoveEntriesNotification(notification => {

            })
            .onEvictEntriesRequest(request => {

            })
            .onEvictEntriesNotification(notification => {

            })
            .onInsertEntriesRequest(request => {

            })
            .onInsertEntriesNotification(notification => {

            })
            .onUpdateEntriesRequest(request => {

            })
            .onUpdateEntriesNotification(notification => {

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