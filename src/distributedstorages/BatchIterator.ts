
type GetBatch<K, V> = (keys: ReadonlySet<K>) => Promise<ReadonlyMap<K, V>>;

export class BatchIterator<K, V> {
    private _keys: K[];
    private _getBatch: GetBatch<K, V>;
    private _batchSize: number;

    public constructor(
        keys: ReadonlySet<K>,
        getBatch: GetBatch<K, V>,
        size: number,
    ) {
        this._keys = Array.from(keys);
        this._getBatch = getBatch;
        this._batchSize = size;
    }

    public async *asyncIterator(): AsyncIterableIterator<[K, V]> {
        let batch: Map<number, [K, V]> | undefined;
        for (let index = 0; index < this._keys.length; ++index) {
            if (batch === undefined || !batch.has(index)) {
                const toIndex = Math.min(index + this._batchSize, this._keys.length);
                const batchKeys = new Set(this._keys.slice(index, toIndex));
                const fetchedEntries = await this._getBatch(batchKeys);
                batch = new Map<number, [K, V]>();
                let savedIndex = index;
                for (const entry of fetchedEntries) {
                    batch.set(savedIndex, entry);
                    ++savedIndex;
                }
            }
            const entry = batch.get(index);
            if (entry) {
                yield entry;
            }
        }
    }
}