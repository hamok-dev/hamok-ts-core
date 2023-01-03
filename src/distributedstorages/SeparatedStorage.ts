import { Collections } from "@hamok-dev/common";
import { splitMap, splitSet } from "@hamok-dev/common/lib/Collections";
import { Storage } from "../storages/Storage";
import { StorageComlink } from "../storages/StorageComlink";
import { StorageEvents, StorageEventsImpl } from "../storages/StorageEvents";



/**
 * Separated by key, assumed that at the same time only one client 
 * mutates entry for a specific key
 */
export class SeparatedStorage<K, V> implements Storage<K, V> {
    private _storage: Storage<K, V>;
    private _comlink: StorageComlink<K, V>;
    private constructor(comlink: StorageComlink<K, V>) {
        this._comlink = comlink
            .onClearEntriesRequest(request => {
                const response = request.createResponse();
                this._storage.clear();
                this._comlink.sendClearEntriesResponse(response);
            })
            .onClearEntriesNotification(notification => {
                this._storage.clear();
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
        let result = await this._storage.get(key);
        if (result !== undefined) {
            return result;
        }
        const remoteEntries = await this._comlink.requestGetEntries(
            Collections.setOf(key)
        );
        if (remoteEntries !== undefined) {
            result = remoteEntries.get(key);
        }
        return result;
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        const localEntries = await this._storage.getAll(keys);
        if (keys.size <= localEntries.size) {
            return localEntries;
        }
        const requests = Collections.splitSet<K>(keys, this._config.maxKeys, () => [keys])
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
        const requests = splitMap<K, V>(
            remainingEntries, 
            Math.min(this._config.maxKeys, this._config.maxValues),
            () => [remainingEntries]
        )
        .map(batchEntries => this._comlink.requestUpdateEntries(batchEntries));
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

    public clearLocalStorage(): Promise<void> {
        return this._storage.clear();
    }

    
}