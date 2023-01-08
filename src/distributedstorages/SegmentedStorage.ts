import { Collections, createLogger } from "@hamok-dev/common";
import { Storage } from "../storages/Storage";
import { StorageComlink, StorageComlinkConfig } from "../storages/StorageComlink";
import { StorageEvents } from "../storages/StorageEvents";
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
    maxKeys: number;
    maxValues: number;
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
    public constructor(
        storage: Storage<K, V>,
        comlink: StorageComlink<K, V>,
        config: SegmentedStorageConfig
    ) {
        this.config = config;
        this._storage = storage;
        this._segments = new Map<K, string>();
        this._comlink = comlink
            .onClearEntriesRequest(async request => {
                const response = request.createResponse();
                this._storage.clear();
                this._comlink.sendClearEntriesResponse(response);
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
                if (request.sourceEndpointId !== this._comlink.localEndpointId) {
                    for (const key of request.keys) {
                        this._segments.delete(key);
                    }
                    return;
                }
                const deletedKeys = await this._storage.deleteAll(request.keys)
                const response = request.createResponse(
                    deletedKeys
                );
                this._comlink.sendDeleteEntriesResponse(response);
            })
            .onDeleteEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onRemoveEntriesRequest(async request => {
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform remove operation without sourceId`);
                    return;
                }
                if (request.sourceEndpointId !== this._comlink.localEndpointId) {
                    for (const key of request.keys) {
                        this._segments.delete(key);
                    }
                    return;
                }
                const removedEntries = await this._storage.getAll(request.keys);
                await this._storage.deleteAll(request.keys);
                const response = request.createResponse(
                    removedEntries
                );
                this._comlink.sendRemoveEntriesResponse(response);
            })
            .onRemoveEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onEvictEntriesRequest(async request => {
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform evict operation without sourceId`);
                    return;
                }
                if (request.sourceEndpointId !== this._comlink.localEndpointId) {
                    for (const key of request.keys) {
                        this._segments.delete(key);
                    }
                    return;
                }
                await this._storage.deleteAll(request.keys);
                const response = request.createResponse();
                this._comlink.sendEvictEntriesResponse(response);
            })
            .onEvictEntriesNotification(async notification => {
                logNotUsedAction(`onDeleteEntriesNotification()`, notification);
            })
            .onInsertEntriesRequest(async request => {
                if (!request.sourceEndpointId) {
                    logger.warn(`Cannot perform insert operation without sourceId`);
                    return;
                }
                const keys = Collections.setFrom(request.entries.keys());
                if (request.sourceEndpointId !== this._comlink.localEndpointId) {
                    for (const key of keys) {
                        const remoteEndpointId = this._segments.get(key);
                        if (remoteEndpointId) continue;
                        this._segments.set(key, request.sourceEndpointId);
                    }
                    return;
                }
                const [segmentKeys, unmatched] = this._groupKeysBySegments(keys);
                const fetchings: Promise<ReadonlyMap<K, V>>[] = [
                    this._storage.getAll(keys)
                ];
                for (const [remoteEndpointId, existingRemoteKeys] of segmentKeys) {
                    const fetchedEntries = this._comlink.requestGetEntries(
                        existingRemoteKeys,
                        Collections.setOf(remoteEndpointId)
                    );
                    fetchings.push(fetchedEntries);
                }
                const existingEntries = Collections.concatMaps<K, V>(
                    new Map<K, V>(),
                    ...(await Promise.all(fetchings))
                );
                const newEntries = Collections.collectEntriesByNotInKeys(
                    request.entries,
                    Collections.setFrom(existingEntries.keys())
                );
                // at this point we force the underlying storage to accept 
                // all entries we considered at this point at this level as new
                await this._storage.setAll(newEntries);
                const response = request.createResponse(existingEntries);
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
                await this._storage.setAll(updatedEntries);
                const response = request.createResponse(updatedEntries);
                this._comlink.sendUpdateEntriesResponse(response);
            })
            .onUpdateEntriesNotification(async notification => {
                logNotUsedAction(`onUpdateEntriesNotification()`, notification);
            })
            .onRemoteEndpointJoined(remoteEndpointId => {

            })
            .onRemoteEndpointDetached(removedRemoteEndpointId => {
                // remove all segments related to the removed remote endpoint id
                for (const [key, remoteEndpointId] of this._segments.entries()) {
                    if (remoteEndpointId !== removedRemoteEndpointId) continue;
                    this._segments.delete(key);
                }
            })
            .onChangedLeaderId(leaderId => {

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
        const [localSize, remoteSize] = await Promise.all([
            this._storage.size(),
            this._comlink.requestGetSize()
        ]);
        return localSize + remoteSize;
    }

    public async isEmpty(): Promise<boolean> {
        if (await this._storage.isEmpty() === false) return false;
        return (await this._comlink.requestGetSize() === 0);
    }

    public async keys(): Promise<ReadonlySet<K>> {
        const [localKeys, remoteKeys] = await Promise.all([
            this._storage.keys(),
            this._comlink.requestGetKeys()
        ]);
        return Collections.reduceSet<K>(
            new Set<K>(),
            collidedKey => {

            },
            localKeys,
            remoteKeys,
        );
    }

    public async clear(): Promise<void> {
        this._storage.clear();
        return this._comlink.requestClearEntries();
    }

    public async get(key: K): Promise<V | undefined> {
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

    public async set(key: K, value: V, thisArg?: any): Promise<V | undefined> {
        const updatedEntries = await this.setAll(Collections.mapOf([key, value]));
        return updatedEntries.get(key);
    }

    public async setAll(entries: ReadonlyMap<K, V>, thisArg?: any): Promise<ReadonlyMap<K, V>> {
        const updatedEntries = await this._comlink.requestUpdateEntries(
            entries,
            Collections.setFrom(
                this._comlink.remoteEndpointIds.keys(),
                Collections.setOf(this._comlink.localEndpointId).keys()
            )    
        );
        const newEntries = Collections.collectEntriesByNotInKeys(
            updatedEntries,
            Collections.setFrom(entries.keys())
        );
        if (newEntries.size < 1) {
            return updatedEntries;
        }
        const nextEntries = Collections.collectEntriesByKeys<K, V>(
            entries,
            Collections.setFrom((await this.insertAll(newEntries)).keys())
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
        const alreadyExistingEntries = await this.insertAll(
            Collections.mapOf([key, value])
        );
        return alreadyExistingEntries.get(key);
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        if (entries.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        return this._comlink.requestInsertEntries(entries);
    }
    
    public async delete(key: K): Promise<boolean> {
        const remoteEndpointId = this._segments.get(key);
        if (remoteEndpointId) {
            const deletedRemoteKeys = await this._comlink.requestDeleteEntries(
                Collections.setOf(key)
            );
            return deletedRemoteKeys.has(key);
        }
        return this._storage.delete(key);
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        const [remoteSegments, shouldBeLocalKeys] = this._groupKeysBySegments(keys);
        const promises: Promise<ReadonlySet<K>>[] = [];
        for (const [remoteEndpointId, remoteKeys] of remoteSegments) {
            const promise = this._comlink.requestDeleteEntries(
                remoteKeys,
                Collections.setOf(remoteEndpointId)
            );
            promises.push(promise);
        }
        if (0 < shouldBeLocalKeys.size) {
            promises.push(this._storage.deleteAll(shouldBeLocalKeys));
        }
        if (promises.length < 1) {
            return Collections.emptySet<K>();
        }
        return Collections.concatSet<K>(
            new Set<K>(),
            ...(await Promise.all(promises))
        );
    }
    
    public async evict(key: K): Promise<void> {
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
        const [remoteSegments, shouldBeLocalKeys] = this._groupKeysBySegments(keys);
        const promises: Promise<void>[] = [];
        for (const [remoteEndpointId, remoteKeys] of remoteSegments) {
            const promise = this._comlink.requestEvictEntries(
                remoteKeys,
                Collections.setOf(remoteEndpointId)
            );
            promises.push(promise);
        }
        if (0 < shouldBeLocalKeys.size) {
            promises.push(this._storage.evictAll(shouldBeLocalKeys));
        }
        if (promises.length < 1) {
            return;
        }
        await Promise.all(promises);
    }
    
    public async restore(key: K, value: V): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        throw new Error("Method not implemented.");
    }
    
    public async *[Symbol.asyncIterator](): AsyncIterableIterator<[K, V]> {
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
        return this._storage[Symbol.asyncIterator];
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