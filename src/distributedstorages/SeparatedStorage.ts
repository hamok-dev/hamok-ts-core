import { Collections, createLogger } from "@hamok-dev/common";
import { StorageSyncResult } from "../HamokGrid";
import { Storage } from "../storages/Storage";
import { StorageComlink } from "../storages/StorageComlink";
import { StorageEvents, StorageEventsImpl } from "../storages/StorageEvents";
import { BatchIterator } from "./BatchIterator";
import { SeparatedStorageBuilder } from "./SeparatedStorageBuilder";

const logger = createLogger(`SeparatedStorage`);

const logNotUsedAction = (context: string, obj: any) => {
    logger.warn(
        `${context}: Incoming message has not been processed, becasue the handler is not implemented`, 
        obj
    );
}

export type SeparatedStorageConfig = {
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
 * Separated by key, assumed that at the same time only one client 
 * mutates entry for a specific key
 */
export class SeparatedStorage<K, V> implements Storage<K, V> {
    public static builder<U, R>(): SeparatedStorageBuilder<U, R> {
        return new SeparatedStorageBuilder<U, R>();
    }

    private _storage: Storage<K, V>;
    private _comlink: StorageComlink<K, V>;
    public readonly config: SeparatedStorageConfig;

    public constructor(
        storage: Storage<K, V>,
        comlink: StorageComlink<K, V>,
        config: SeparatedStorageConfig
    ) {
        this.config = config;
        this._storage = storage;
        this._comlink = comlink
            .onClearEntriesRequest(async request => {
                const response = request.createResponse();
                this._storage.clear();
                this._comlink.sendClearEntriesResponse(response);
            })
            .onClearEntriesNotification(async notification => {
                this._storage.clear();
                logger.info(`onClearEntriesNotification(): Entries in ${this.id} are cleared`, notification);
            })
            .onGetEntriesRequest(async request => {
                // logger.info("Get request", request);
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
                    "onDeleteEntriesNotification()",
                    notification
                );
                // await this._storage.deleteAll(notification.keys);
            })
            .onRemoveEntriesRequest(async request => {
                logNotUsedAction(
                    "onRemoveEntriesRequest()",
                    request
                );
            })
            .onRemoveEntriesNotification(async notification => {
                logNotUsedAction(
                    "onRemoveEntriesNotification()",
                    notification
                );
            })
            .onEvictEntriesRequest(async request => {
                await this._storage.evictAll(request.keys);
                this._comlink.sendEvictEntriesResponse(request.createResponse())
            })
            .onEvictEntriesNotification(async notification => {
                await this._storage.evictAll(notification.keys);
            })
            .onInsertEntriesRequest(async request => {
                logNotUsedAction(
                    "onInsertEntriesRequest()",
                    request
                );
            })
            .onInsertEntriesNotification(async notification => {
                logNotUsedAction(
                    "onDeleteEntriesNotification()",
                    notification
                );
            })
            .onUpdateEntriesRequest(async request => {
                const keys = Collections.setFrom(request.entries.keys());
                const oldEntries = await this._storage.getAll(keys);
                const updatedEntries = new Map<K, V>();
                for (const [key, value] of oldEntries) {
                    const newValue = request.entries.get(key);
                    if (newValue) {
                        updatedEntries.set(key, newValue);
                    }
                }
                if (0 < updatedEntries.size) {
                    await this._storage.setAll(updatedEntries);
                }
                const response = request.createResponse(oldEntries);
                this._comlink.sendUpdateEntriesResponse(response);
            })
            .onUpdateEntriesNotification(async notification => {
                logNotUsedAction(
                    "onUpdateEntriesNotification()",
                    notification
                );
            })
            .onStorageSyncRequested(async promise => {
                this._executeSync()
                    .then(promise.resolve)
                    .catch(promise.reject)
            })
            // .onRemoteEndpointJoined(remoteEndpointId => {

            // })
            // .onRemoteEndpointDetached(remoteEndpointId => {

            // })
            // .onChangedLeaderId(leaderId => {

            // })
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
        let result: V | undefined = await this._storage.get(key);
        if (result !== undefined) {
            return result;
        }
        const remoteEntries = await this._comlink.requestGetEntries(
            Collections.setOf(key)
        );
        if (remoteEntries !== undefined) {
            // console.warn(remoteEntries);
            result = remoteEntries.get(key);
        }
        return result;
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        const localEntries = await this._storage.getAll(keys);
        // console.warn("getAll", keys, localEntries);
        if (keys.size <= localEntries.size) {
            return localEntries;
        }
        const requests = Collections.splitSet<K>(keys, this.config.maxKeys, () => [keys])
            .map(chunk => this._comlink.requestGetEntries(chunk))
            ;
        const result = new Map<K, V>(localEntries);
        (await Promise.all(requests))
            .forEach(entries => Collections.reduceMaps(
                result,
                (key, v1, v2) => {
                    // duplicated item is found
                    // detectedCollisionsSubject::onNext,
                },
                entries
            ));
        return result;
    }
    
    public async set(key: K, value: V): Promise<V | undefined> {
        const localEntry = await this._storage.get(key);
        if (localEntry) {
            return this._storage.set(key, value);
        }
        const remoteEntries = await this._comlink.requestUpdateEntries(
            new Map<K, V>([[key, value]])
        );
        if (remoteEntries.has(key)) {
            return remoteEntries.get(key);
        }
        return this._storage.set(key, value);
    }
    
    public async setAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        const missingKeys = Collections.setFrom(entries.keys());
        const updatedLocalEntries = await this._storage.getAll(missingKeys);
        if (0 < updatedLocalEntries.size) {
            const updatedEntries = new Map<K, V>();
            for (const key of updatedLocalEntries.keys()) {
                const value = entries.get(key);
                if (!value) continue;
                updatedEntries.set(key, value);
                missingKeys.delete(key);
            }
        }
        if (missingKeys.size < 1) {
            return updatedLocalEntries;
        }
        const remainingEntries = new Map<K, V>();
        for (const key of missingKeys) {
            const value = entries.get(key);
            if (!value) continue;
            remainingEntries.set(key, value);
        }
        const requests = Collections.splitMap<K, V>(
            remainingEntries, 
            Math.min(this.config.maxKeys, this.config.maxValues),
            () => [remainingEntries]
        ).map(batchEntries => this._comlink.requestUpdateEntries(batchEntries));
        
        const result = new Map<K, V>();
        for await (const updatedRemoteEntries of requests) {
            for (const [key, value] of updatedRemoteEntries.entries()) {
                missingKeys.delete(key);
                result.set(key, value);
            }
        }
        const newEntries = new Map<K, V>();
        for (const key of missingKeys) {
            const value = entries.get(key);
            if (!value) continue;
            newEntries.set(key, value);
        }
        if (0 < newEntries.size) {
            await this._storage.setAll(newEntries)
        }
        return result;

    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        const alreadyExistingEntries = await this.insertAll(Collections.mapOf<K, V>([key, value]));
        return alreadyExistingEntries.get(key);
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        const missingKeys = Collections.setFrom(entries.keys());
        const existingLocalEntries = await this._storage.getAll(missingKeys);
        if (0 < existingLocalEntries.size) {
            existingLocalEntries.forEach((_value, key) => missingKeys.delete(key));
        }
        if (missingKeys.size < 1) {
            return existingLocalEntries;
        }
        const requests = Collections.splitSet<K>(
            missingKeys, 
            this.config.maxKeys,
            () => [missingKeys]
        ).map(batchKeys => this._comlink.requestGetEntries(batchKeys));
        
        const existingRemoteEntries = new Map<K, V>();
        for await (const remoteEntries of requests) {
            for (const [key, value] of remoteEntries.entries()) {
                missingKeys.delete(key);
                existingRemoteEntries.set(key, value);
            }
        }

        const result = Collections.mapFrom<K, V>(
            existingLocalEntries.entries(), 
            existingRemoteEntries.entries()
        );
        if (missingKeys.size < 1) {
            return result;
        }

        const newEntries = new Map<K, V>();
        for (const key of missingKeys) {
            const value = entries.get(key);
            if (!value) continue;
            newEntries.set(key, value);
        }
        return Collections.mapFrom<K, V>(
            result.entries(),
            (await this._storage.insertAll(newEntries)).entries()
        )
    }
    
    public async delete(key: K): Promise<boolean> {
        if ((await this._storage.delete(key)) === true) {
            return true;
        }
        const deletedKeys = await this._comlink.requestDeleteEntries(Collections.setOf(key));
        return deletedKeys.has(key);
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        if (keys.size < 1) {
            return Collections.emptySet<K>();
        }
        const localDeletedKeys = await this._storage.deleteAll(keys);
        if (localDeletedKeys.size === keys.size) {
            return localDeletedKeys;
        }
        let remainingKeys: ReadonlySet<K> | undefined;
        if (localDeletedKeys.size < 1) {
            remainingKeys = localDeletedKeys;
        } else {
            const keys = new Set<K>();
            for (const key of keys) {
                if (!localDeletedKeys.has(key)) {
                    keys.add(key)
                }
            }
            remainingKeys = keys;
        }

        const requests = Collections.splitSet<K>(
            remainingKeys, 
            this.config.maxKeys,
            () => [remainingKeys ?? new Set<K>()]
        ).map(batchKeys => this._comlink.requestDeleteEntries(batchKeys));
        
        const result = new Set<K>(localDeletedKeys);
        for await (const deletedKeys of requests) {
            for (const key of deletedKeys) {
                result.add(key)
            }
        }
        return result;
    }
    
    public async evict(key: K): Promise<void> {
        throw new Error("Evict operation is not allowed");
    }
    
    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        throw new Error("Evict operation is not allowed");
    }
    
    public async restore(key: K, value: V): Promise<void> {
        throw new Error("Restore operation is not allowed");
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        throw new Error("Restore operation is not allowed");
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
        return this._storage.isEmpty();
    }

    public localStorageSize(): Promise<number> {
        return this._storage.size();
    }

    public localStorageKeys(): Promise<ReadonlySet<K>> {
        return this._storage.keys();
    }

    public async *localStorageEntries(): AsyncIterableIterator<[K, V]> {
        return this._storage;
    }

    public localClearStorage(): Promise<void> {
        return this._storage.clear();
    }

    public async checkCollidingEntries() {
        await this._checkCollidingEntries(this._comlink.remoteEndpointIds).catch(err => {
            logger.warn(`Error occurred while checking colliding entries`);
        });
    }
    
    private async _checkCollidingEntries(remoteEndpointIds: ReadonlySet<string>): Promise<void> {
        if (remoteEndpointIds.size < 1) {
            return;
        }
        const localKeys = await this._storage.keys();
        if (localKeys.size < 1) {
            return;
        }
        const collidingKeysToRemoteLocally = new Set<K>();
        const collidingKeysToRemoveRemotely = new Set<K>();
        for (const remoteEndpointId of remoteEndpointIds) {
            const remoteEntries = await this._comlink.requestGetEntries(
                localKeys,
                Collections.setOf<string>(remoteEndpointId)
            ).catch(err => {
                logger.warn(`_checkCollidingEntries(): Error occurred while collecting entries from remote endpoint ${remoteEndpointId}`, err);
                return undefined;
            });
            if (!remoteEntries) {
                continue;
            }
            for (const localKey of localKeys) {
                if (!remoteEntries.has(localKey)) {
                    continue;
                }
                if (this._comlink.localEndpointId.localeCompare(remoteEndpointId) < 0) {
                    collidingKeysToRemoveRemotely.add(localKey);
                } else {
                    collidingKeysToRemoteLocally.add(localKey);
                }
            }
        }
        if (collidingKeysToRemoteLocally.size < 1 && collidingKeysToRemoveRemotely.size < 1) {
            return;
        }
        const remoteRequests = collidingKeysToRemoveRemotely.size < 1 
            ? [Promise.resolve()]
            : Collections.splitSet<K>(
                collidingKeysToRemoveRemotely, 
                this.config.maxKeys,
                () => [collidingKeysToRemoveRemotely]
            ).map(batchKeys => this._comlink.requestEvictEntries(batchKeys));
        const localRequest = collidingKeysToRemoteLocally.size < 1
            ? Promise.resolve()
            : this._storage.evictAll(collidingKeysToRemoteLocally)
        ;

        await Promise.all([
            localRequest,
            ...remoteRequests
        ]);
    }

    private async _executeSync(): Promise<StorageSyncResult> {
        const remoteEndpointIds = this._comlink.remoteEndpointIds;
        let success = true;
        let evictedEntries = 0;
        const errors: string[] = [];
        const localKeys = await this._storage.keys();
        const collidingKeys = new Set<K>();
        if (localKeys.size < 1) {
            logger.info(`Storage Sync has been performed on storage: ${this.id}. Removed Entries: 0, New local storage size: 0`);
            return {
                success: true
            };
        }
        for (const remoteEndpointId of remoteEndpointIds) {
            const remoteEntries = await this._comlink.requestGetEntries(localKeys).catch(err => {
                logger.warn(`_executeSync(): Error occurred while requesting remote entries from endpoint ${remoteEndpointId}`, err);
                success = false;
                errors.push(err);
                return undefined;
            })
            if (remoteEntries === undefined) {
                continue;
            }
            for (const localKey of localKeys) {
                const remoteEntry = remoteEntries.get(localKey);
                if (!remoteEntry) {
                    continue;
                }
                collidingKeys.add(localKey);
            }
        }
        if (0 < collidingKeys.size) {
            await this._storage.evictAll(collidingKeys);
            evictedEntries += collidingKeys.size;
        }
        logger.info(`Storage Sync has been performed on storage ${this.id}. Evicted entries ${evictedEntries}, new local storage size: ${await this._storage.size()}`);
        return {
            success,
            errors: success ? undefined : errors
        }
    }
}