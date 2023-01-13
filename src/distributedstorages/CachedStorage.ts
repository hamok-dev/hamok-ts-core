import { ClearEntriesNotification, Collections, createLogger, DeleteEntriesNotification, EvictEntriesNotification } from "@hamok-dev/common";
import { Storage } from "../storages/Storage";
import { StorageComlink, StorageComlinkConfig } from "../storages/StorageComlink";
import { StorageEvents } from "../storages/StorageEvents";
import { CacheMetrics, CacheMetricsImpl } from "./Cache";
import { CachedStorageBuilder } from "./CachedStorageBuilder";

const logger = createLogger(`CachedStorage`);
const logNotUsedAction = (context: string, obj: any) => {
    logger.warn(
        `${context}: Incoming message has not been processed, becasue the handler is not implemented`, 
        obj
    );
}

export type CachedStorageConfig = StorageComlinkConfig & {
    /**
     * The maximum number of keys can be put into one outgoing request / response
     */
    maxKeys: number,
    /**
     * The maximum number of values can be put into one outgoing request / response
     */
    maxValues: number,
}

/**
 * Wraps a Storage and Caches it to a map, but maps are shared
 * accross the grid
 */
export class CachedStorage<K, V> implements Storage<K, V> {
    public static builder<U, R>(): CachedStorageBuilder<U, R> {
        return new CachedStorageBuilder<U, R>();
    }

    public config: CachedStorageConfig;
    private _metrics = new CacheMetricsImpl();
    private _storage: Storage<K, V>;
    private _cache: Map<K, V>;
    private _comlink: StorageComlink<K, V>;
    private _pendingFetches: Map<K, Promise<V | undefined>>;

    public constructor(
        storage: Storage<K, V>,
        cache: Map<K, V>,
        comlink: StorageComlink<K, V>,
        config: CachedStorageConfig,
    ) {
        this.config = config;
        this._storage = storage;
        this._cache = cache;
        this._pendingFetches = new Map<K, Promise<V | undefined>>();
        this._comlink = comlink
            .onClearEntriesRequest(async request => {
                logNotUsedAction("onInsertEntriesNotification()", request);
            })
            .onClearEntriesNotification(async notification => {
                this._cache.clear();
                logger.debug(`onClearEntriesNotification(): Cleared the cache`, notification);
            })
            .onGetEntriesRequest(async request => {
                const foundEntries = new Map<K, V>();
                const pendingFetches: Promise<void>[] = [];
                for (const key of request.keys) {
                    const value = this._cache.get(key);
                    if (value) {
                        foundEntries.set(key, value);
                    }
                    const pendingFetch = this._pendingFetches.get(key);
                    if (pendingFetch) {
                        const promise = pendingFetch.then<void>(value => {
                            if (value) {
                                foundEntries.set(key, value);
                            }
                        });
                        pendingFetches.push(promise);
                    }
                }
                if (0 < pendingFetches.length) {
                    const allFetches = Promise.all(pendingFetches);
                    this._comlink.addOngoingRequestId(request.requestId, request.sourceEndpointId)
                    allFetches.finally(() => {
                        this._comlink.removeOngoingRequestId(request.requestId);
                    });
                    await allFetches;
                }
                const response = request.createResponse(
                    foundEntries
                );
                this._comlink.sendGetEntriesResponse(response);
            })
            .onGetSizeRequest(async request => {
                logNotUsedAction("onGetSizeRequest()", request);
            })
            .onGetKeysRequest(async request => {
                logNotUsedAction("onGetKeysRequest()", request);
            })
            .onDeleteEntriesRequest(async request => {
                logNotUsedAction("onDeleteEntriesRequest()", request);
            })
            .onDeleteEntriesNotification(async notification => {
                logNotUsedAction("onDeleteEntriesNotification()", notification);
            })
            .onRemoveEntriesRequest(async request => {
                logNotUsedAction("onRemoveEntriesRequest()", request);
            })
            .onRemoveEntriesNotification(async notification => {
                logNotUsedAction("onRemoveEntriesNotification()", notification);
            })
            .onEvictEntriesRequest(async request => {
                logNotUsedAction("onEvictEntriesRequest()", request);
            })
            .onEvictEntriesNotification(async notification => {
                for (const key of notification.keys) {
                    this._cache.delete(key);
                }
            })
            .onInsertEntriesRequest(async request => {
                logNotUsedAction("onInsertEntriesRequest()", request);
            })
            .onInsertEntriesNotification(async notification => {
                logNotUsedAction("onInsertEntriesNotification()", notification);
            })
            .onUpdateEntriesRequest(async request => {
                logNotUsedAction("onUpdateEntriesRequest()", request);
            })
            .onUpdateEntriesNotification(async notification => {
                logNotUsedAction("onUpdateEntriesNotification()", notification);
            })
            .onStorageSyncRequested(async promise => {
                this._cache.clear();
                promise.resolve({
                    success: true,
                });
            })
            ;
    }

    public get id(): string {
        return this._storage.id;
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

        await Promise.all([
            this._comlink.sendClearEntriesNotification(new ClearEntriesNotification()),
            this._storage.clear()
        ]);
    }

    public async get(key: K): Promise<V | undefined> {
        let value = this._cache.get(key);
        if (value) {
            this._metrics.incrementLocalHits();
            return value;
        }
        const remoteEntries = await this._comlink.requestGetEntries(
            Collections.setOf(key)
        );
        value = remoteEntries.get(key);
        if (value) {
            this._metrics.incrementRemoteHits();
            return value;
        }
        let pendingFetch = this._pendingFetches.get(key);
        if (!pendingFetch) {
            pendingFetch = this._storage.get(key);
            pendingFetch.then(value => {
                if (value) {
                    this._metrics.incrementMisses();
                    this._cache.set(key, value);
                }
            }).finally(() => {
                this._pendingFetches.delete(key);
            });
            this._pendingFetches.set(key, pendingFetch);
        }
        return await pendingFetch;
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        const localCachedEntries = new Map<K, V>();
        const missingKeys = new Set<K>();
        for (const key of keys) {
            const localValue = this._cache.get(key);
            if (localValue) {
                localCachedEntries.set(key, localValue);
            } else {
                missingKeys.add(key)
            }
        }
        if (0 < localCachedEntries.size) {
            this._metrics.incrementLocalHits(localCachedEntries.size);
        }
        if (missingKeys.size < 1) {
            return localCachedEntries;
        }
        const remoteCachedEntries = await this._comlink.requestGetEntries(missingKeys);
        if (0 < remoteCachedEntries.size) {
            this._metrics.incrementRemoteHits(remoteCachedEntries.size);
        }
        for (const key of remoteCachedEntries.keys()) {
            missingKeys.delete(key);
        }
        if (missingKeys.size < 1) {
            return Collections.concatMaps(
                new Map<K, V>(),
                localCachedEntries,
                remoteCachedEntries
            );
        }
        this._metrics.incrementMisses(missingKeys.size);
        const pendingFetch = this._storage.getAll(missingKeys);
        pendingFetch.finally(() => {
            for (const missingKey of missingKeys) {
                this._pendingFetches.delete(missingKey);
            }    
        });
        for (const missingKey of missingKeys) {
            this._pendingFetches.set(missingKey, new Promise(resolve => {
                pendingFetch.then(entries => resolve(entries.get(missingKey)))
            }));
        }
        return Collections.concatMaps(
            new Map<K, V>(),
            localCachedEntries,
            remoteCachedEntries,
            await pendingFetch
        );
    }
    
    public async set(key: K, value: V): Promise<V | undefined> {
        const result = await this.setAll(
            Collections.mapOf([key, value])
        );
        return result.get(key);
    }
    
    public async setAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        const keys = Collections.setFrom(entries.keys());
        for (const key of keys) {
            this._cache.delete(key);
        }
        const [ result ] = await Promise.all([
            this._storage.setAll(entries),
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(
                keys
            ))
        ]);
        return result;
    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        const result = await this.insertAll(
            Collections.mapOf([key, value])
        );
        return result.get(key);
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        const keys = Collections.setFrom(entries.keys());
        for (const key of keys) {
            this._cache.delete(key);
        }
        const [ result ] = await Promise.all([
            this._storage.insertAll(entries),
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(
                keys
            ))
        ]);
        return result;
    }
    
    public async delete(key: K): Promise<boolean> {
        this._cache.delete(key);
        const [result] = await Promise.all([
            this._storage.delete(key),
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(
                Collections.setOf(key)
            ))
        ])
        return result;
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        if (keys.size < 1) {
            return Collections.emptySet<K>();
        }
        keys.forEach(key => this._cache.delete(key));
        const [result] = await Promise.all([
            this._storage.deleteAll(keys),
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(keys))
        ]);
        return result;
    }
    
    public async evict(key: K): Promise<void> {
        this._cache.delete(key);
        await Promise.all([
            this._storage.evict(key),
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(
                Collections.setOf(key)
            ))
        ]);
    }
    
    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        keys.forEach(key => this._cache.delete(key));
        await Promise.all([
            this._storage.evictAll(keys),
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(keys))
        ]);
    }
    
    public async restore(key: K, value: V): Promise<void> {
        await Promise.all([
            this._comlink.sendEvictEntriesNotification(new EvictEntriesNotification(
                Collections.setOf(key)
            )),
            this._storage.restore(key, value)
        ]);
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        const notification = new EvictEntriesNotification(
            Collections.setFrom(entries.keys())
        );
        await Promise.all([
            this._storage.restoreAll(entries),
            this._comlink.sendEvictEntriesNotification(notification)
        ]);
    }
    
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<[K, V]> {
        return this._storage[Symbol.asyncIterator];
    }

}