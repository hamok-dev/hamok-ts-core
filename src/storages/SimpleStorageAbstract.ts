import { Collections } from "@hamok-dev/common";
import { Storage } from "./Storage";
import { StorageEvents, StorageEventsImpl } from "./StorageEvents";

export abstract class SimpleStorageAbstract<K, V> implements Storage<K, V> {
    readonly id: string;
    private _events = new StorageEventsImpl<K, V>();

    public constructor(id: string) {
        this.id = id;
    }
    public get events(): StorageEvents<K, V> {
        return this._events;
    }

    public async get(key: K): Promise<V | undefined> {
        const entries = await this.getAll(Collections.setOf(key));
        return entries.get(key);
    }
    
    public async set(key: K, value: V): Promise<V | undefined> {
        const entries = await this.setAll(Collections.mapOf([key, value]));
        return entries.get(key);
    }
    
    public async setAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        const existingEntries = await this.getAll(
            new Set<K>(Array.from(entries.keys()))
        );
        const updatedEntries = Collections.collectEntriesByKeys<K, V>(
            entries,
            new Set<K>(Array.from(entries.keys()))
        );
        const newEntries = Collections.collectEntriesByNotInKeys(
            entries,
            new Set<K>(existingEntries.keys())
        );
        if (0 < existingEntries.size) {
            await this.update(updatedEntries);
            Array.from(updatedEntries).forEach(([key, newValue]) => {
                const oldValue = existingEntries.get(key)!;
                this._events.emitUpdatedEntry({
                    key,
                    oldValue,
                    newValue
                });
            });
        }
        if (newEntries.size < 1) {
            return existingEntries;
        }
        await this.create(newEntries);
        Array.from(newEntries).forEach(([key, value]) => {
            this._events.emitCreatedEntry({
                key,
                value
            });
        });
        return existingEntries;
    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        const entries = await this.insertAll(Collections.mapOf([key, value]));
        return entries.get(key);
    }
    
    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        const existingEntries = await this.getAll(
            new Set<K>(Array.from(entries.keys()))
        );
        const newEntries = Collections.collectEntriesByNotInKeys(
            entries,
            new Set<K>(existingEntries.keys())
        );
        await this.create(newEntries);
        Array.from(newEntries).forEach(([key, value]) => {
            this._events.emitCreatedEntry({
                key,
                value
            });
        });
        return existingEntries;
    }
    
    public async delete(key: K): Promise<boolean> {
        const entries = await this.deleteAll(Collections.setOf(key));
        return entries.has(key);
    }
    
    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        const deltedEntries = await this.getAll(keys);
        await this.remove(keys);
        Array.from(deltedEntries).forEach(([key, value]) => {
            this._events.emitDeletedEntry({
                key,
                value
            });
        });
        return new Set<K>(Array.from(deltedEntries.keys()));
    }
    
    public async evict(key: K): Promise<void> {
        await this.evictAll(Collections.setOf(key));
    }
    
    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        const evictedEntries = await this.getAll(keys);
        await this.remove(keys);
        Array.from(evictedEntries).forEach(([key, value]) => {
            this._events.emitEvictedEntry({
                key,
                value
            });
        });
    }
    
    public async restore(key: K, value: V): Promise<void> {
        await this.restoreAll(Collections.mapOf([key, value]));
    }
    
    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        const existingEntries = await this.getAll(
            new Set<K>(Array.from(entries.keys()))
        );
        const newEntries = Collections.collectEntriesByNotInKeys(
            entries,
            new Set<K>(existingEntries.keys())
        );
        await this.create(newEntries);
        Array.from(newEntries).forEach(([key, value]) => {
            this._events.emitRestoredEntry({
                key,
                value
            });
        });
    }
    
    public abstract size(): Promise<number>;
    public abstract isEmpty(): Promise<boolean>;
    public abstract keys(): Promise<ReadonlySet<K>>;
    public abstract clear(): Promise<void>;
    public abstract getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>>;
    public abstract [Symbol.asyncIterator](): AsyncIterableIterator<[K, V]>;
    protected abstract create(entries: ReadonlyMap<K, V>): Promise<void>;
    protected abstract update(entries: ReadonlyMap<K, V>): Promise<void>;
    protected abstract remove(keys: ReadonlySet<K>): Promise<void>;
}