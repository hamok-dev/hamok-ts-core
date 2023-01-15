import { Collections, createLogger } from "@hamok-dev/common";
import { Storage } from "../storages/Storage";
import { StorageComlink, StorageComlinkConfig } from "../storages/StorageComlink";
import { StorageEvents } from "../storages/StorageEvents";
import { CompletablePromise } from "../utils/CompletablePromise";
import { BatchIterator } from "./BatchIterator";
import { SegmentedStorageBuilder } from "./SegmentedStorageBuilder";

const logger = createLogger("SegmentedStorage");
const logNotUsedAction = (context: string, obj: any) => {
    logger.warn(
        `${context}: Incoming message has not been processed, becasue the handler is not implemented`, 
        obj
    );
}


export type SegmentedStorageConfig = StorageComlinkConfig & {
    /**
     * The maximum number of keys can be put into one outgoing request / response
     */
    maxKeys: number,
    /**
     * The maximum number of values can be put into one outgoing request / response
     */
    maxValues: number,
    /**
     * The maximum recursion depth for certain actions (set, setAll) 
     * allowed without throwing an exception
     */
    maxRecursionRetry: number;
}

/**
 * Segmented by key, all key replicated to all distributed storages,
 * but the value only stored on one, the other have the destination id
 * for that segmented storage
 */
export class SegmentedStorage<K, V> implements Storage<K, V> {
    public static builder<U, R>(): SegmentedStorageBuilder<U, R> {
        return new SegmentedStorageBuilder<U, R>();
    }

    public readonly config: SegmentedStorageConfig;
    private _segments: Map<K, string>;
    private _storage: Storage<K, V>;
    private _comlink: StorageComlink<K, V>;
    private _standalone: boolean;
    private _ongoingSync?: Promise<void>;

    public constructor(
        storage: Storage<K, V>,
        comlink: StorageComlink<K, V>,
        config: SegmentedStorageConfig
    ) {
        this.config = config;
        this._standalone = true;
        this._storage = storage;
        this._segments = new Map<K, string>();
        this._comlink = comlink
            .onClearEntriesRequest(async request => {
                this._storage.clear();
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse();
                    this._comlink.sendClearEntriesResponse(response);
                }
            })
            .onClearEntriesNotification(async notification => {
                logNotUsedAction(`onClearEntriesNotification()`, notification);
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
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform delete operation without sourceId`);
                    return;
                }
                const deletedKeys = new Set<K>();
                for (const key of request.keys) {
                    if (this._segments.delete(key)) {
                        deletedKeys.add(key);
                    }
                }
                Collections.concatSet(
                    deletedKeys,
                    await this._storage.deleteAll(request.keys)
                );
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse(
                        deletedKeys
                    );
                    this._comlink.sendDeleteEntriesResponse(response);
                }
            })
            .onDeleteEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onRemoveEntriesRequest(async request => {
                logNotUsedAction(`onRemoveEntriesRequest()`, request);
            })
            .onRemoveEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onEvictEntriesRequest(async request => {
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform evict operation without sourceId`);
                    return;
                }
                for (const key of request.keys) {
                    this._segments.delete(key);
                }
                await this._storage.evictAll(request.keys);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse();
                    this._comlink.sendEvictEntriesResponse(response);
                }
            })
            .onEvictEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onInsertEntriesRequest(async request => {
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform insert operation without sourceId`);
                    return;
                }
                const existingKeys = new Set<K>();
                const segmentKeys = new Map<string, K[]>();
                const newEntries = new Map<K, V>();
                for (const [key, value] of request.entries) {
                    const remoteEndpointId = this._segments.get(key);
                    if (!remoteEndpointId) {
                        newEntries.set(key, value);
                        continue;
                    }
                    let keys = segmentKeys.get(remoteEndpointId);
                    if (!keys) {
                        keys = [];
                        segmentKeys.set(remoteEndpointId, keys);
                    }
                    keys.push(key);
                    existingKeys.add(key);
                }
                if (request.sourceEndpointId !== this._comlink.localEndpointId) {
                    for (const key of newEntries.keys()) {
                        this._segments.set(key, request.sourceEndpointId);
                    }
                    return;
                }
                const alreadyExistingEntries = 0 < existingKeys.size
                    ? await this.getAll(existingKeys)
                    : Collections.emptyMap<K, V>();

                const response = request.createResponse(alreadyExistingEntries);
                if (newEntries.size < 1) {
                    this._comlink.sendInsertEntriesResponse(response);
                    return;
                }
                // at this point we force the underlying storage to accept 
                // all entries we considered at this point at this level as new
                await this._storage.setAll(newEntries);
                this._comlink.sendInsertEntriesResponse(response);
            })
            .onInsertEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onUpdateEntriesRequest(async request => {
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform update operation without sourceId`);
                    return;
                }
                const keys = Collections.setFrom(request.entries.keys());
                const oldEntries = await this._storage.getAll(keys);
                const updatedEntries = new Map<K, V>();
                for (const [key] of oldEntries) {
                    const newValue = request.entries.get(key);
                    if (!newValue) continue;
                    updatedEntries.set(key, newValue);
                }
                if (0 < updatedEntries.size) {
                    await this._storage.setAll(updatedEntries);
                }
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    const response = request.createResponse(updatedEntries);
                    this._comlink.sendUpdateEntriesResponse(response);
                }
            })
            .onUpdateEntriesNotification(async notification => {
                logNotUsedAction(`onUpdateEntriesNotification()`, notification);
            })
            // .onRemoteEndpointJoined(remoteEndpointId => {

            // })
            .onRemoteEndpointDetached(removedRemoteEndpointId => {
                // remove all segments related to the removed remote endpoint id
                for (const [key, remoteEndpointId] of this._segments.entries()) {
                    if (remoteEndpointId !== removedRemoteEndpointId) continue;
                    this._segments.delete(key);
                }
            })
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
                logger.trace(`Storage ${this.id} is joining to the grid`);
                this._ongoingSync = this._storage.keys()
                    .then(keys => this._storage.getAll(keys))
                    .then(entries => {
                        this._storage.clear().then(() => new Promise<void>(resolve => {
                            this._standalone = false;
                            this.insertAll(entries).then(() => resolve());
                        }));
                    })
                this._ongoingSync.finally(() => {
                    this._ongoingSync = undefined;
                });
            })
            .onStorageSyncRequested(async promise => {
                const endpointIds: string[] = [];
                const promises: Promise<ReadonlySet<K>>[] = [];
                for (const remoteEndpointId of this._comlink.remoteEndpointIds) {
                    promises.push(this._comlink.requestGetKeys(
                        Collections.setOf(remoteEndpointId)
                    ));
                    endpointIds.push(remoteEndpointId);
                }
                if (promises.length < 1) {
                    promise.resolve({
                        success: true,
                    });
                    return;
                }
                const arrayOfKeys = await Promise.all(promises);
                for (let i = 0; i < Math.min(arrayOfKeys.length, endpointIds.length); ++i) {
                    const remoteEndpointId = endpointIds[i];
                    for (const key of arrayOfKeys[i]) {
                        this._segments.set(key, remoteEndpointId);
                    }
                }
            })
            ;
    }

    public get id(): string {
        return this._storage.id;
    }
    
    public get events(): StorageEvents<K, V> {
        return this._storage.events;
    }

    public async size(): Promise<number> {
        if (this._standalone) {
            return this._storage.size();
        }
        const [localSize, remoteSize] = await Promise.all([
            this._storage.size(),
            this._comlink.requestGetSize()
        ]);
        return localSize + remoteSize;
    }

    public async isEmpty(): Promise<boolean> {
        if (this._standalone) {
            return this._storage.isEmpty();
        }
        if (await this._storage.isEmpty() === false) return false;
        return (await this._comlink.requestGetSize() === 0);
    }

    public async keys(): Promise<ReadonlySet<K>> {
        if (this._standalone) {
            return this._storage.keys();
        }
        const [localKeys, remoteKeys] = await Promise.all([
            this._storage.keys(),
            this._comlink.requestGetKeys()
        ]);
        return Collections.reduceSet<K>(
            new Set<K>(),
            collidedKey => {
                logger.warn(`Colliding entries in storage ${this.id} for key ${collidedKey}`);
            },
            localKeys,
            remoteKeys,
        );
    }

    public async clear(): Promise<void> {
        await this._storage.clear();
        if (!this._standalone) {
            return this._comlink.requestClearEntries();
        }
    }

    public async get(key: K): Promise<V | undefined> {
        if (this._standalone) {
            return this._storage.get(key);
        }
        const remoteEndpointId = this._segments.get(key);
        if (remoteEndpointId) {
            const remoteEntries = await this._comlink.requestGetEntries(
                Collections.setOf(key),
                Collections.setOf(remoteEndpointId)
            );
            return remoteEntries.get(key);
        }
        return await this._storage.get(key);
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        if (keys.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        if (this._standalone) {
            return this._storage.getAll(keys);
        }
        const [segmentKeys, unmatched] = this._groupKeysBySegments(keys);
        const promises: Promise<ReadonlyMap<K, V>>[] = [];
        for (const [remoteEndpointId, remoteKeys] of segmentKeys) {
            promises.push(
                this._comlink.requestGetEntries(
                    remoteKeys,
                    Collections.setOf(remoteEndpointId)
                )
            );
        }
        if (0 < unmatched.size) {
            promises.push(
                this._storage.getAll(unmatched)
            )
        }
        if (promises.length < 1) {
            return Collections.emptyMap<K, V>();
        }
        return Collections.mapFrom<K, V>(
            ...((await Promise.all(promises)).map(map => map.entries()))
        );
    }

    public async set(key: K, value: V): Promise<V | undefined> {
        if (this._standalone) {
            return this._storage.set(key, value);
        }
        const updatedEntries = await this.setAll(Collections.mapOf([key, value]));
        return updatedEntries.get(key);
    }

    /* eslint-disable @typescript-eslint/no-explicit-any */
    public async setAll(entries: ReadonlyMap<K, V>, thisArg?: any): Promise<ReadonlyMap<K, V>> {
        if (this._standalone) {
            return this._storage.setAll(entries);
        }
        const updatedEntries = Collections.concatMaps(
            new Map<K, V>(),
            (await this.getAll(Collections.setFrom(entries.keys())))
        );
        const newEntries = Collections.collectEntriesByNotInKeys(
            entries,
            Collections.setFrom(updatedEntries.keys())
        );
        const requests: Promise<ReadonlyMap<K, V>>[] = [];
        if (0 < updatedEntries.size) {
            Collections.splitMap<K, V>(
                entries, 
                Math.min(this.config.maxKeys, this.config.maxValues),
                () => [entries]
            ).map(batchEntries => this._comlink.requestUpdateEntries(
                batchEntries,
                Collections.setOf(this._comlink.localEndpointId)
            )).forEach(request => requests.push(request));
        }
        await Promise.all(requests);
        if (newEntries.size < 1) {
            return updatedEntries;
        }
        const alreadyExistingEntries = await this.insertAll(newEntries);
        const nextEntries = Collections.collectEntriesByKeys<K, V>(
            entries,
            Collections.setFrom(alreadyExistingEntries.keys())
        );
        if (nextEntries.size < 1) {
            return updatedEntries;
        }
        const lastEntriesSize = thisArg?.lastEntriesSize ?? entries.size;
        const shrinking = nextEntries.size < lastEntriesSize;
        const retried = shrinking ? 0 : (thisArg?.retried ?? 0) + 1;
        if (this.config.maxRecursionRetry < retried) {
            throw new Error(`setAll() Failed, because of too many recursive calls of the same number of entries tried to be updated`);
        }
        return Collections.concatMaps(
            new Map<K, V>(),
            updatedEntries,
            await this.setAll(nextEntries, {
                lastEntriesSize,
                retried
            })
        );
    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        if (this._standalone) {
            return this._storage.insert(key, value);
        }
        const alreadyExistingEntries = await this.insertAll(
            Collections.mapOf([key, value])
        );
        return alreadyExistingEntries.get(key);
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        if (entries.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        if (this._standalone) {
            return this._storage.insertAll(entries);
        }
        return this._comlink.requestInsertEntries(
            entries,
            Collections.setOf(this._comlink.localEndpointId)
        );
    }
    
    public async delete(key: K): Promise<boolean> {
        if (this._standalone) {
            return this._storage.delete(key);
        }
        const deletedKeys = await this._comlink.requestDeleteEntries(
            Collections.setOf(key),
            Collections.setOf(this._comlink.localEndpointId)
        );
        return deletedKeys.has(key);
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        if (keys.size < 1) {
            return Collections.emptySet<K>();
        }
        if (this._standalone) {
            return this._storage.deleteAll(keys);
        }

        const result = await this._comlink.requestRemoveEntries(
            keys,
            Collections.setOf(this._comlink.localEndpointId)
        )
        return Collections.setFrom(result.keys());
    }
    
    public async evict(key: K): Promise<void> {
        if (this._standalone) {
            return this._storage.evict(key);
        }
        const remoteEndpointId = this._segments.get(key);
        if (remoteEndpointId) {
            await this._comlink.requestEvictEntries(
                Collections.setOf(key)
            );
            return;
        }
        await this._storage.evict(key);
    }
    
    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        if (keys.size < 1) {
            return;
        }
        if (this._standalone) {
            return this._storage.evictAll(keys);
        }
        return this._comlink.requestEvictEntries(
            keys,
            Collections.setOf(this._comlink.localEndpointId)
        );
    }
    
    public async restore(key: K, value: V): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<[K, V]> {
        if (this._standalone) {
            return this._storage[Symbol.asyncIterator];
        }
        const keys = await this.keys();
        const batchIterator = new BatchIterator(
            keys,
            this.getAll.bind(this),
            this.config.maxValues
        );
        for await (const entry of batchIterator.asyncIterator()) {
            yield entry;
        }
    }

    public async isLocalStorageEmpty(): Promise<boolean> {
        return await this._storage.isEmpty();
    }

    public localStorageSize(): Promise<number> {
        return this._storage.size();
    }

    public localStorageKeys(): Promise<ReadonlySet<K>> {
        return this._storage.keys();
    }

    public async *localStorageEntries(): AsyncIterableIterator<[K, V]> {
        const iterator = this._storage[Symbol.asyncIterator]();
        for await (const entry of iterator) {
            yield entry;
        }
    }

    public async clearLocalStorage(): Promise<void> {
        await this._storage.clear();
    }
 
    
    private _groupKeysBySegments(keys: ReadonlySet<K>): [Map<string, ReadonlySet<K>>, ReadonlySet<K>] {
        const segmentKeys = new Map<string, Set<K>>();
        const unmatched = new Set<K>();
        for (const key of keys) {
            const remoteEndpointId = this._segments.get(key);
            if (!remoteEndpointId) {
                unmatched.add(key);
                continue;
            }
            let group = segmentKeys.get(remoteEndpointId);
            if (!group) {
                group = new Set<K>();
                segmentKeys.set(remoteEndpointId, group);
            }
            group.add(key);
        }
        return [segmentKeys, unmatched];
    }
}