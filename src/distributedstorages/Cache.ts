export interface CacheMetrics {
    readonly localHits: number;
    readonly remoteHits: number;
    readonly misses: number;
    reset(): void;
}

export class CacheMetricsImpl implements CacheMetrics {
    private _localHits = 0;
    private _remoteHits = 0;
    private _misses = 0;
    
    public reset(): void {
        this._localHits = 0;
        this._remoteHits = 0;
        this._misses = 0;
    }

    public get localHits(): number {
        return this._localHits;
    }

    public get remoteHits(): number {
        return this._remoteHits;
    }

    public incrementLocalHits(value?: number): void {
        this._localHits += value ?? 1;
    }

    public incrementRemoteHits(value?: number): void {
        this._remoteHits += value ?? 1;
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