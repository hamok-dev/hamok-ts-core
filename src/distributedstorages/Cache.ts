export interface CacheMetrics {
    readonly hits: number;
    readonly misses: number;
    reset(): void;
}

export class CacheMetricsImpl implements CacheMetrics {
    private _hits = 0;
    private _misses = 0;
    
    public reset(): void {
        this._hits = 0;
        this._misses = 0;
    }

    public get hits(): number {
        return this._hits;
    }

    public incrementHits(value?: number): void {
        this._hits += value ?? 1;
    }

    public get misses(): number {
        return this._misses;
    }

    public incrementMisses(value?: number): void {
        this._misses += value ?? 1;
    }
}

export interface Cache<K, V> {
    get(key: K): Promise<V | undefined>;
    getAll(keys: ReadonlySet<K>): Promise<ReadonlyMap<K, V>>;
    put(key: K, value: V): Promise<void>;
    putAll(entries: ReadonlyMap<K, V>): Promise<void>;
    delete(key: K): Promise<boolean>
    deleteAll(keys: ReadonlySet<K>): Promise<boolean>
    clear(): Promise<void>;

}