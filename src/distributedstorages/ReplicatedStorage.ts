import { Collections, createLogger } from "@hamok-dev/common";
import { Storage } from "../storages/Storage";
import { StorageComlink, StorageComlinkConfig } from "../storages/StorageComlink";
import { StorageEvents } from "../storages/StorageEvents";
import { ReplicatedStorageBuilder } from "./ReplicatesStorageBuilder";

const logger = createLogger("ReplicatedStorage");

/* eslint-disable  @typescript-eslint/no-explicit-any */
const logNotUsedAction = (context: string, obj: any) => {
    logger.warn(
        `${context}: Incoming message has not been processed, becasue the handler is not implemented`, 
        obj
    );
}

export type ReplicatedStorageConfig = StorageComlinkConfig & {
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
 * Replicated storage replicates all entries on all distributed storages
 */
export class ReplicatedStorage<K, V> implements Storage<K, V> {
    public static builder<U, R>(): ReplicatedStorageBuilder<U, R> {
        return new ReplicatedStorageBuilder<U, R>();
    }

    public readonly config: ReplicatedStorageConfig;
    private _standalone: boolean;
    private _storage: Storage<K, V>;
    private _comlink: StorageComlink<K, V>;
    private _ongoingSync?: Promise<void>;

    public constructor(
        config: ReplicatedStorageConfig,
        storage: Storage<K, V>,
        comlink: StorageComlink<K, V>
    ) {
        this.config = config;
        this._standalone = comlink.remoteEndpointIds.size < 1;
        this._storage = storage;
        this._comlink = comlink
            .onClearEntriesRequest(async request => {
                this._storage.clear();
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse();
                    this._comlink.sendClearEntriesResponse(response);
                }
            })
            .onClearEntriesNotification(async notification => {
                logNotUsedAction(
                    "onClearEntriesNotification", 
                    notification
                );
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
                const deletedKeys = await this._storage.deleteAll(request.keys);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse(deletedKeys);
                    this._comlink.sendDeleteEntriesResponse(response);
                }
            })
            .onDeleteEntriesNotification(async notification => {
                logNotUsedAction(
                    "onDeleteEntriesNotification()",
                    notification,
                );
            })
            .onRemoveEntriesRequest(async request => {
                logNotUsedAction(
                    "onRemoveEntriesRequest()",
                    request,
                );
            })
            .onRemoveEntriesNotification(async notification => {
                logNotUsedAction(
                    "onRemoveEntriesNotification()",
                    notification,
                );
            })
            .onEvictEntriesRequest(async request => {
                await this._storage.evictAll(request.keys);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse();
                    this._comlink.sendEvictEntriesResponse(response);
                }
            })
            .onEvictEntriesNotification(async notification => {
                logNotUsedAction(
                    "onEvictEntriesNotification()",
                    notification,
                );
            })
            .onInsertEntriesRequest(async request => {
                const alreadyExistingEntries = await this._storage.insertAll(request.entries);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse(alreadyExistingEntries);
                    this._comlink.sendInsertEntriesResponse(response);
                }
            })
            .onInsertEntriesNotification(async notification => {
                logNotUsedAction(
                    "onInsertEntriesNotification()",
                    notification,
                );
            })
            .onUpdateEntriesRequest(async request => {
                const updatedEntries = await this._storage.setAll(request.entries);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse(updatedEntries);
                    this._comlink.sendUpdateEntriesResponse(response);
                }
            })
            .onUpdateEntriesNotification(async notification => {
                logNotUsedAction(
                    "onUpdateEntriesNotification()",
                    notification,
                );
            })
            // .onRemoteEndpointJoined(remoteEndpointId => {
                
            // })
            // .onRemoteEndpointDetached(remoteEndpointId => {

            // })
            .onChangedLeaderId(async leaderId => {
                if (leaderId === undefined) {
                    return;
                }
                const wasAlone = this._standalone;
                if (!wasAlone) return;
                if (this._ongoingSync) {
                    await this._ongoingSync;
                    return;
                }
                this._ongoingSync = new Promise(resolve => {
                    logger.info(`Storage ${this.id} is joining to the grid`);
                    this._standalone = false;
                    this._clearAndSetAllEntries().then(resolve)
                });
                this._ongoingSync.catch(err => {
                    logger.warn(`onChangedLeaderId(): Error occurred while performing operation`, err);
                }).finally(() => {
                    this._ongoingSync = undefined;
                });
            })
            .onStorageSyncRequested(async promise => {
                if (this._ongoingSync) {
                    await this._ongoingSync;
                    return;
                }
                logger.info(`Storage ${this.id} is being synchronized`);
                this._ongoingSync = this._clearAndSetAllEntries();
                this._ongoingSync.then(() => {
                    promise.resolve({
                        success: true,
                    })
                }).catch(err => {
                    logger.warn(`onChangedLeaderId(): Error occurred while performing operation`, err);
                    promise.resolve({
                        success: false,
                        errors: [err]
                    })
                }).finally(() => {
                    this._ongoingSync = undefined;
                });
            })
            ;
    }

    private _clearAndSetAllEntries(): Promise<void> {
        return this._storage.keys()
            .then(keys => this._storage.getAll(keys))
            .then(entries => new Promise<void>(resolve => {
                this._storage.clear()
                    .then(() => this.setAll(entries))
                    .then(() => resolve())
            }));
    }

    public get id(): string {
        return this._storage.id;
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
        if (this._standalone) {
            return this._storage.clear();
        }
        return this._comlink.requestClearEntries();
    }

    public async get(key: K): Promise<V | undefined> {
        return this._storage.get(key);
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        return this._storage.getAll(keys);
    }
    
    public async set(key: K, value: V): Promise<V | undefined> {
        const result = await this.setAll(
            Collections.mapOf([key, value])
        );
        return result.get(key);
    }
    
    public async setAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        if (entries.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        if (this._standalone) {
            return this._storage.setAll(entries);
        }
        const requests = Collections.splitMap<K, V>(
            entries,
            Math.min(this.config.maxKeys, this.config.maxValues),
            () => [entries]
        ).map(batchedEntries => this._comlink.requestUpdateEntries(
            batchedEntries,
            Collections.setOf(this._comlink.localEndpointId)
        ));
        return Collections.concatMaps(
            new Map<K, V>(),
            ...(await Promise.all(requests))
        );
    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        const result = await this.insertAll(
            Collections.mapOf([key, value])
        );
        return result.get(key);
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        if (entries.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        if (this._standalone) {
            return this._storage.insertAll(entries);
        }
        const requests = Collections.splitMap<K, V>(
            entries,
            Math.min(this.config.maxKeys, this.config.maxValues),
            () => [entries]
        ).map(batchedEntries => this._comlink.requestInsertEntries(
            batchedEntries,
            Collections.setOf(this._comlink.localEndpointId)
        ));
        return Collections.concatMaps(
            new Map<K, V>(),
            ...(await Promise.all(requests))
        );
    }
    
    public async delete(key: K): Promise<boolean> {
        const result = await this.deleteAll(
            Collections.setOf(key)
        );
        return result.has(key);
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        if (keys.size < 1) {
            return Collections.emptySet<K>();
        }
        if (this._standalone) {
            return this._storage.deleteAll(keys);
        }
        const requests = Collections.splitSet<K>(
            keys,
            Math.min(this.config.maxKeys, this.config.maxValues),
            () => [keys]
        ).map(batchedEntries => this._comlink.requestDeleteEntries(
            batchedEntries,
            Collections.setOf(this._comlink.localEndpointId)
        ));
        return Collections.concatSet<K>(
            new Set<K>(),
            ...(await Promise.all(requests))
        );
    }
    
    public async evict(key: K): Promise<void> {
        return this.evictAll(
            Collections.setOf(key)
        );
    }
    
    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        if (keys.size < 1) {
            return;
        }
        if (this._standalone) {
            return this._storage.evictAll(keys);
        }
        const requests = Collections.splitSet<K>(
            keys,
            Math.min(this.config.maxKeys, this.config.maxValues),
            () => [keys]
        ).map(batchedEntries => this._comlink.requestDeleteEntries(
            batchedEntries,
            Collections.setOf(this._comlink.localEndpointId)
        ));
        await Promise.all(requests);
    }
    
    public async restore(key: K, value: V): Promise<void> {
        return this.restoreAll(
            Collections.mapOf([key, value])
        );
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        if (entries.size < 1) {
            return;
        }
        if (this._standalone) {
            return this._storage.restoreAll(entries);
        }
        throw new Error(`Not implemented`);
        // const requests = Collections.splitMap<K, V>(
        //     entries,
        //     Math.min(this.config.maxKeys, this.config.maxValues)
        // ).map(batchedEntries => this._comlink.r(
        //     batchedEntries,
        //     Collections.setOf(this._comlink.localEndpointId)
        // ));
        // await Promise.all(requests);
    }
    
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<[K, V]> {
        const iterator = this._storage[Symbol.asyncIterator]();
        for await (const entry of iterator) {
            yield entry;
        }
    }
}