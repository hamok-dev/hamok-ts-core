import { 
    Collections 
} from "@hamok-dev/common"
import { Storage } from "./Storage";
import { StorageEvents, StorageEventsImpl } from "./StorageEvents";
import { FailedOperationException } from "../FailedOperationException";
import { SlidingMap } from "../SlidingMap";

interface Builder<K, V> {
    setId(value: string): Builder<K, V>;
    setBaseMap(map: Map<K, V>): Builder<K, V>;
    setSlidingMap(map: SlidingMap<K, V>): Builder<K, V>;
    build(): SimpleStorage<K, V>;
}

export class SimpleStorage<K, V> implements Storage<K, V> {
    public static builder<U, R>(): Builder<U, R> {
        const storage = new SimpleStorage<U, R>();
        const result: Builder<U, R> = {
            setId: (value: string) => {
                storage._id = value;
                return result;
            },
            setBaseMap: (baseMap: Map<U, R>) => {
                storage._entries = baseMap;
                return result;
            },
            setSlidingMap: (slidingMap: SlidingMap<U, R>) => {
                storage._entries = slidingMap;
                slidingMap.onExpiredEntry((key, value) => {
                    storage._events.emitExpiredEntry({
                        key,
                        value
                    });
                });
                return result;
            },
            build: () => {
                if (!storage._id) {
                    throw new Error(`Cannot build Storage without identifier`);
                }
                return storage;
            }
        };
        return result;
    }

    private _id?: string;
    private _entries = new Map<K, V>();
    private _events = new StorageEventsImpl<K, V>();
    private constructor() {
        
    }
    public get id(): string {
        return this._id!;
    }
    
    public get events(): StorageEvents<K, V> {
        return this._events;
    }

    public async keys(): Promise<ReadonlySet<K>> {
        const result = new Set<K>(this._entries.keys());
        return result;
    }

    public async size(): Promise<number> {
        return this._entries.size;
    }

    public async isEmpty(): Promise<boolean> {
        return this._entries.size == 0;
    }


    public async clear(): Promise<void> {
        const clearedEntries = new Map<K, V>(this._entries);
        this._entries.clear();
        for (const [key, value] of clearedEntries.entries()) {
            this._events.emitEvictedEntry({
                key,
                value
            });
        }
    }

    public async get(key: K): Promise<V | undefined> {
        return this._entries.get(key);
    }

    public async getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>> {
        if (keys.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        const result = new Map<K, V>();
        for (const key of keys) {
            const value = this._entries.get(key);
            if (value) {
                result.set(key, value);
            }
        }
        // console.warn("simpleStorage ", keys, result, this._entries, Collections.unmodifiableMap(result));
        return Collections.unmodifiableMap(result);
    }

    public async set(key: K, value: V): Promise<V | undefined> {
        const oldValue = this._entries.get(key);
        this._entries.set(key, value);
        if (!oldValue) {
            this._events.emitCreatedEntry({
                key, 
                value,
            });
        } else {
            this._events.emitUpdatedEntry({
                key,
                oldValue,
                newValue: value,
            })
        }
        return oldValue;
    }

    public async setAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        if (entries.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        const result = new Map<K, V>();
        for (const [key, value] of entries) {
            const oldValue = this._entries.get(key);
            this._entries.set(key, value);
            if (!oldValue) {
                this._events.emitCreatedEntry({
                    key, 
                    value,
                });
                continue;
            }
            this._events.emitUpdatedEntry({
                key,
                oldValue,
                newValue: value,
            });
            result.set(key, oldValue);
        }
        return Collections.unmodifiableMap<K, V>(result);
    }
    
    public async insert(key: K, value: V): Promise<V | undefined> {
        const existingValue = this._entries.get(key);
        if (existingValue) {
            return existingValue;
        }
        this._entries.set(key, value);
        this._events.emitCreatedEntry({
            key,
            value
        });
    }

    public async insertAll(entries: ReadonlyMap<K, V>): Promise<ReadonlyMap<K, V>> {
        if (entries.size < 1) {
            return Collections.emptyMap<K, V>();
        }
        const result = new Map<K, V>();
        for (const [key, value] of entries) {
            const existingValue = this._entries.get(key);
            if (existingValue) {
                result.set(key, existingValue);
                continue;
            }
            this._entries.set(key, value);
            this._events.emitCreatedEntry({
                key,
                value
            });
        }
        return Collections.unmodifiableMap(result);
    }

    public async delete(key: K): Promise<boolean> {
        const value = this._entries.get(key);
        if (!value) {
            return false;
        }
        if (!this._entries.delete(key)) {
            return false;
        }
        this._events.emitDeletedEntry({
            key,
            value
        });
        return true;
    }

    public async deleteAll(keys: ReadonlySet<K>): Promise<ReadonlySet<K>> {
        const result = new Set<K>();
        for (const key of keys) {
            const value = this._entries.get(key);
            if (!value) {
                continue;
            }
            if (!this._entries.delete(key)) {
                continue;
            }
            result.add(key);
            this._events.emitDeletedEntry({
                key,
                value
            });
        }
        return Collections.unmodifiableSet<K>(result);
    }

    public async evict(key: K): Promise<void> {
        const value = this._entries.get(key);
        if (!value) {
            return;
        }
        if (!this._entries.delete(key)) {
            return;
        }
        this._events.emitEvictedEntry({
            key,
            value
        });
    }

    public async evictAll(keys: ReadonlySet<K>): Promise<void> {
        for (const key of keys) {
            const value = this._entries.get(key);
            if (!value) {
                continue;
            }
            if (!this._entries.delete(key)) {
                continue;
            }
            this._events.emitEvictedEntry({
                key,
                value
            });
        }
    }

    public async restore(key: K, value: V): Promise<void> {
        if (this._entries.has(key)) {
            throw new FailedOperationException(
                `Cannot restore already presented entries. key: ${key}, value: ${value}`
            );
        }
        this._entries.set(key, value);
        this._events.emitRestoredEntry({
            key,
            value,
        });
    }

    public async restoreAll(entries: ReadonlyMap<K, V>): Promise<void> {
        for (const [key, value] of entries) {
            if (this._entries.has(key)) {
                throw new FailedOperationException(
                    `Cannot restore already presented entries. key: ${key}, value: ${value}`
                );
            }
            this._entries.set(key, value);
            this._events.emitRestoredEntry({
                key,
                value,
            });
        }
    }

    public async *[Symbol.asyncIterator](): AsyncIterableIterator<[K, V]> {
        return this._entries.entries();
    }

}