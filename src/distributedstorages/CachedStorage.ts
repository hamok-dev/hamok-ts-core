import { Storage } from "../storages/Storage";
import { StorageComlink } from "../storages/StorageComlink";
import { StorageEvents, StorageEventsImpl } from "../storages/StorageEvents";
import { CacheMetrics, CacheMetricsImpl } from "./Cache";

/**
 * Wraps a Storage and Caches it to a map, but maps are shared
 * accross the grid
 */
export class CachedStorage<K, V> implements Storage<K, V> {
    
    private _metrics = new CacheMetricsImpl();
    private _storage: Storage<K, V>;
    private _cache: Map<K, V>;
    private _comlink: StorageComlink<K, V>;

    private constructor(
        storage: Storage<K, V>,
        cache: Map<K, V>,
        comlink: StorageComlink<K, V>
    ) {
        this._storage = storage;
        this._cache = cache;
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

    public get metrics(): CacheMetrics {
        return this._metrics;
    }
    
    public get events(): StorageEvents<K, V> {
        return this._storage.events;
    }

    public async size(): Promise<number> {
        return this._storage.size();
    }

    public async isEmpty(): Promise<boolean> {
        return this._storage.isEmpty();
    }

    public async keys(): Promise<ReadonlySet<K>> {
        return this._storage.keys();
    }

    public async clear(): Promise<void> {
        this._cache.clear();
        return this._storage.clear();
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