import { Collections } from "@hamok-dev/common";
import EventEmitter from "events";

export interface SlidingSet<T> extends Set<T> {
    refresh(): number;
    onExpiredEntry(listener: (value: T) => void): this;
    offExpiredEntry(listener: (value: T) => void): this;
}

interface Builder<T> {
    setExpirationTime(valueInMs: number, createTimer?: boolean, tickTimeInMs?: number): Builder<T>;
    setMaxItems(value: number): Builder<T>;
    build(): SlidingSet<T>;
}

const EXPIRED_ENTRY_EVENT_NAME = "EXPIRED_ENTRY_EVENT_NAME";

export class SimpleSlidingSet<T> implements SlidingSet<T> {

    public static builder<U>(): Builder<U> {
        const slidingMap = new SimpleSlidingSet<U>();
        const result: Builder<U> = {
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
    private _entries = new Map<T, number>();
    
    private _createTimer: boolean;
    private _tickTimeInMs = 0;
    private _timer?: ReturnType<typeof setInterval>;
    private _maxTimeInMs = 0;
    private _maxEntries = 0;

    private constructor() {
        this._createTimer = false;
    }

    public onExpiredEntry(listener: (item: T) => void): this {
        this._emitter.on(EXPIRED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    public offExpiredEntry(listener: (item: T) => void): this {
        this._emitter.off(EXPIRED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    public clear(): void {
        this._entries.clear();
    }

    public delete(item: T): boolean {
        const result = this._entries.delete(item);
        if (this._entries.size < 1) {
            this._deinitTimer();
        }
        return result;
    }
    /* eslint-disable @typescript-eslint/no-explicit-any */
    public forEach(callbackfn: (value: T, value2: T, set: Set<T>) => void, thisArg?: any): void {
        Collections.setFrom(this._entries.keys()).forEach(callbackfn, thisArg);
    }

    public has(item: T): boolean {
        return this._entries.has(item);
    }

    public add(item: T): this {
        if (this._entries.size === 0) {
            this._initTimer();
        }
        this._entries.set(item, Date.now());
        this.refresh();
        return this;
    }

    public get size(): number {
        return this._entries.size;
    }

    public [Symbol.iterator](): IterableIterator<T> {
        return this._entries.keys();
    }
    
    public values(): IterableIterator<T> {
        return this._entries.keys();
    }

    public entries(): IterableIterator<[T, T]> {
        return Collections.setFrom(this._entries.keys()).entries();
    }

    public keys(): IterableIterator<T> {
        return this._entries.keys();
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
        for (const [key, touched] of Array.from(this._entries)) {
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
        const sortedTouches = [...this._entries.entries()].sort((a, b) => a[1] - b[1]);
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