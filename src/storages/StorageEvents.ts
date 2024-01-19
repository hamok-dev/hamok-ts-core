import EventEmitter from "events"

const CREATED_ENTRY_EVENT_NAME = "createdEntry";
const UPDATED_ENTRY_EVENT_NAME = "updatedEntry";
const DELETED_ENTRY_EVENT_NAME = "deletedEntry";
const EXPIRED_ENTRY_EVENT_NAME = "expiredEntry";
const EVICTED_ENTRY_EVENT_NAME = "evictedEntry";
const RESTORED_ENTRY_EVENT_NAME = "restoredEntry";



type EventTypes = 
    "createdEntry" |
    "updatedEntry" |
    "deletedEntry" |
    "expiredEntry" |
    "evictedEntry" |
    "restoredEntry"

export type CreatedEntryEvent<K, V> = {
    key: K;
    value: V;
}

export type UpdatedEntryEvent<K, V> = {
    key: K;
    oldValue: V;
    newValue: V;
}

export type DeletedEntryEvent<K, V> = {
    key: K;
    value: V;
}

export type ExpiredEntryEvent<K, V> = {
    key: K;
    value: V;
}

export type EvictedEntryEvent<K, V> = {
    key: K;
    value: V;
}

export type RestoredEntryEvent<K, V> = {
    key: K;
    value: V;
}

export type CreatedEntryListener<K, V> = (event: CreatedEntryEvent<K, V>) => void;
export type UpdatedEntryListener<K, V> = (event: UpdatedEntryEvent<K, V>) => void;
export type DeletedEntryListener<K, V> = (event: DeletedEntryEvent<K, V>) => void;
export type ExpiredEntryListener<K, V> = (event: ExpiredEntryEvent<K, V>) => void;
export type EvictedEntryListener<K, V> = (event: EvictedEntryEvent<K, V>) => void;
export type RestoredEntryListener<K, V> = (event: RestoredEntryEvent<K, V>) => void;

export interface StorageEvents<K, V> {
    onCreatedEntry(listener: CreatedEntryListener<K, V>): StorageEvents<K, V>;
    onUpdatedEntry(listener: UpdatedEntryListener<K, V>): StorageEvents<K, V>;
    onDeletedEntry(listener: DeletedEntryListener<K, V>): StorageEvents<K, V>;
    onExpiredEntry(listener: ExpiredEntryListener<K, V>): StorageEvents<K, V>;
    onEvictedEntry(listener: EvictedEntryListener<K, V>): StorageEvents<K, V>;
    onRestoredEntry(listener: RestoredEntryListener<K, V>): StorageEvents<K, V>;
}



export class StorageEventsImpl<K, V> implements StorageEvents<K, V> {

    private _emitter = new EventEmitter();

    onCreatedEntry(listener: CreatedEntryListener<K, V>): StorageEvents<K, V> {
        this._emitter.on(CREATED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    onUpdatedEntry(listener: UpdatedEntryListener<K, V>): StorageEvents<K, V> {
        this._emitter.on(UPDATED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    onDeletedEntry(listener: DeletedEntryListener<K, V>): StorageEvents<K, V> {
        this._emitter.on(DELETED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    onExpiredEntry(listener: ExpiredEntryListener<K, V>): StorageEvents<K, V> {
        this._emitter.on(EXPIRED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    onEvictedEntry(listener: EvictedEntryListener<K, V>): StorageEvents<K, V> {
        this._emitter.on(EVICTED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    onRestoredEntry(listener: RestoredEntryListener<K, V>): StorageEvents<K, V> {
        this._emitter.on(RESTORED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    emitCreatedEntry(event: CreatedEntryEvent<K, V>): void {
        this._emitter.emit(CREATED_ENTRY_EVENT_NAME, event);
    }

    emitUpdatedEntry(event: UpdatedEntryEvent<K, V>): void {
        this._emitter.emit(UPDATED_ENTRY_EVENT_NAME, event);
    }

    emitDeletedEntry(event: DeletedEntryEvent<K, V>): void {
        this._emitter.emit(DELETED_ENTRY_EVENT_NAME, event);
    }

    emitExpiredEntry(event: ExpiredEntryEvent<K, V>): void {
        this._emitter.emit(EXPIRED_ENTRY_EVENT_NAME, event);
    }

    emitEvictedEntry(event: EvictedEntryEvent<K, V>): void {
        this._emitter.emit(EVICTED_ENTRY_EVENT_NAME, event);
    }

    emitRestoredEntry(event: RestoredEntryEvent<K, V>): void {
        this._emitter.emit(RESTORED_ENTRY_EVENT_NAME, event);
    }
}

