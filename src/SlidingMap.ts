import EventEmitter from "events";

export interface SlidingMap<K, V> extends Map<K, V> {
    refresh(): number;
    onExpiredEntry(listener: (key: K, value: V) => void): this;
    offExpiredEntry(listener: (key: K, value: V) => void): this;
}

interface Builder<K, V> {
    setBaseMap(map: Map<K, V>): Builder<K, V>;
    setExpirationTime(valueInMs: number, createTimer?: boolean, tickTimeInMs?: number): Builder<K, V>;
    setMaxItems(value: number): Builder<K, V>;
    build(): SlidingMap<K, V>;
}

const EXPIRED_ENTRY_EVENT_NAME = "EXPIRED_ENTRY_EVENT_NAME";

export class SimpleSlidingMap<K, V> implements SlidingMap<K, V> {

    public static builder<K, V>(): Builder<K, V> {
        const slidingMap = new SimpleSlidingMap<K, V>();
        const result: Builder<K, V> = {
            setBaseMap: (value: Map<K, V>) => {
                slidingMap._entries = value;
                return result;
            },
            setExpirationTime: (valueInMs: number, createTimer?: boolean, tickTimeInMs?: number) => {
                slidingMap._maxTimeInMs = valueInMs;
                if (createTimer === true) {
                    slidingMap._createTimer = true;
                    slidingMap._tickTimeInMs = tickTimeInMs ?? 1000;
                }
                return result;
            },
            setMaxItems: (value: number) => {
                slidingMap._maxEntries = value;
                return result;
            },
            build: () => {
                if (slidingMap._maxEntries < 1 && slidingMap._maxTimeInMs < 1) {
                    throw new Error(`SlidingMap must be limited by either time or size`);
                }
                return slidingMap;
            }
        }
        return result;
    }

    private _emitter = new EventEmitter();
    private _entries = new Map<K, V>();
    private _touches = new Map<K, number>();
    
    private _createTimer: boolean;
    private _tickTimeInMs = 0;
    private _timer?: ReturnType<typeof setInterval>;
    private _maxTimeInMs = 0;
    private _maxEntries = 0;

    private constructor() {
        this._createTimer = false;
    }

    public onExpiredEntry(listener: (key: K, value: V) => void): this {
        this._emitter.on(EXPIRED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    public offExpiredEntry(listener: (key: K, value: V) => void): this {
        this._emitter.off(EXPIRED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    public clear(): void {
        this._entries.clear();
        this._touches.clear();
    }

    public delete(key: K): boolean {
        this._touches.delete(key);
        const result = this._entries.delete(key);
        if (this._entries.size < 1) {
            this._deinitTimer();
        }
        return result;
    }

    public forEach(callbackfn: (value: V, key: K, map: Map<K, V>) => void, thisArg?: any): void {
        this._entries.forEach(callbackfn, thisArg);
    }

    public get(key: K): V | undefined {
        const result = this._entries.get(key);
        if (result) {
            this._touches.set(key, Date.now());
        }
        return result;
    }

    public has(key: K): boolean {
        return this._entries.has(key);
    }

    public set(key: K, value: V): this {
        if (this._entries.size === 0) {
            this._initTimer();
        }
        this._entries.set(key, value);
        this._touches.set(key, Date.now());
        this.refresh();
        return this;
    }

    public get size(): number {
        return this._entries.size;
    }

    public [Symbol.iterator](): IterableIterator<[K, V]> {
        return this._entries[Symbol.iterator]();
    }
    
    public entries(): IterableIterator<[K, V]> {
        return this._entries.entries();
    }

    public keys(): IterableIterator<K> {
        return this._entries.keys();
    }
    
    public values(): IterableIterator<V> {
        return this._entries.values();
    }

    public get [Symbol.toStringTag](): string  {
        return this._entries[Symbol.toStringTag];
    }

    public refresh(): number {
        let result = 0;
        if (0 < this._maxTimeInMs) {
            result += this._refreshByTime();
        }
        if (0 < this._maxEntries) {
            result += this._refreshBySize();
        }
        return result;
    }


    private _refreshByTime(): number {
        if (this._maxTimeInMs < 1) {
            return 0;
        }
        let result = 0;
        const threshold = Date.now() - this._maxTimeInMs;
        for (const [key, touched] of Array.from(this._touches)) {
            if (threshold < touched) {
                continue;
            }
            const value = this._entries.get(key);
            if (this.delete(key)) {
                this._emitter.emit(EXPIRED_ENTRY_EVENT_NAME, key, value);
                ++result;
            }
        }
        return result;
    }

    private _refreshBySize(): number {
        if (this._maxEntries < 1 || this._entries.size <= this._maxEntries) {
            return 0;
        }
        let result = 0;
        const overflowed = this._maxEntries - this._entries.size;
        const sortedTouches = [...this._touches.entries()].sort((a, b) => a[1] - b[1]);
        for (let i = 0; i < overflowed && i < sortedTouches.length; ++i) {
            const [key] = sortedTouches[i];
            result += this.delete(key) ? 1 : 0;
        }
        return result;
    }

    private _initTimer(): void {
        if (!this._createTimer || this._tickTimeInMs < 1 || this._timer) {
            return;
        }
        this._timer = setInterval(() => {
            this._refreshByTime();
        }, this._tickTimeInMs);
    }

    private _deinitTimer(): void {
        if (!this._timer) {
            return;
        }
        clearInterval(this._timer);
        this._timer = undefined;
    }
}