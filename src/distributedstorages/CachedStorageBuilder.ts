import { createCodec, Message, MessageProtocol, StorageCodec } from "@hamok-dev/common";
import { HamokGrid } from "../HamokGrid";
import { SimpleStorage } from "../storages/SimpleStorage";
import { Storage } from "../storages/Storage";
import { StorageComlinkConfig, StorageDecoder, StorageEncoder } from "../storages/StorageComlink";
import { CachedStorage, CachedStorageConfig } from "./CachedStorage";
import { v4 as uuid } from "uuid";
import { ResponseChunkerImpl } from "../messages/ResponseChunkerImpl";

export type SeparatedStorageBuildingConfig = StorageComlinkConfig & CachedStorageConfig;

export class CachedStorageBuilder<K, V> {
    private readonly _generatedStorageId: string;
    private readonly _generatedRequestTimeoutInMs: number;
    private _config: SeparatedStorageBuildingConfig;
    private _grid?: HamokGrid;
    private _baseStorage?: Storage<K, V>;
    private _cache?: Map<K, V>;
    private _keyEncoder?: StorageEncoder<K>;
    private _keyDecoder?: StorageDecoder<K>;
    private _valueEncoder?: StorageEncoder<V>;
    private _valueDecoder?: StorageDecoder<V>;

    public constructor() {
        this._generatedStorageId = uuid();
        this._generatedRequestTimeoutInMs = Math.random();
        this._config = {
            requestTimeoutInMs: this._generatedRequestTimeoutInMs,
            neededResponse: 0,
            throwExceptionOnTimeout: true,
            storageId: this._generatedStorageId,
            maxKeys: 0,
            maxValues: 0,
            ongoingRequestsSendingPeriodInMs: 0,
            synchronize: {
                storageSync: false,
                clearEntries: false,
                getEntries: false,
                getKeys: false,
                getSize: false,
                deleteEntries: false,
                removeEntries: false,
                evictEntries: false,
                insertEntries: false,
                updateEntries: false
            },
        }
    }

    public get baseStorage(): Storage<K, V> | undefined {
        return this._baseStorage;
    }

    public setBaseStorage(storage?: Storage<K, V>): CachedStorageBuilder<K, V> {
        if (!storage) return this;
        this._baseStorage = storage;
        this._config.storageId = storage.id;
        return this;
    }

    public setHamokGrid(grid: HamokGrid): CachedStorageBuilder<K, V> {
        this._grid = grid;
        return this;
    }

    public withKeyEncoder(encoder: StorageEncoder<K>): CachedStorageBuilder<K, V> {
        this._keyEncoder = encoder;
        return this;
    }
    
    public withKeyDecoder(decoder: StorageDecoder<K>): CachedStorageBuilder<K, V> {
        this._keyDecoder = decoder;
        return this;
    }
    
    public withValueEncoder(encoder: StorageEncoder<V>): CachedStorageBuilder<K, V> {
        this._valueEncoder = encoder;
        return this;
    }
    
    public withValueDecoder(decoder: StorageDecoder<V>): CachedStorageBuilder<K, V> {
        this._valueDecoder = decoder
        return this;
    }

    public withConfig(partialConfig: Partial<SeparatedStorageBuildingConfig>): CachedStorageBuilder<K, V> {
        Object.assign(this._config, partialConfig);
        return this;
    }
    
    public build(): CachedStorage<K, V> {
        if (!this._baseStorage) {
            this._baseStorage = SimpleStorage.builder<K, V>()
                .setId(this._config.storageId)
                .build();
        } else {
            this._config.storageId = this._baseStorage.id;
        }
        if (this._config.storageId === this._generatedStorageId) {
            throw new Error(`Cannot build a CachedStorage without a given storageId`);
        }
        if (!this._keyEncoder || 
            !this._keyDecoder || 
            !this._valueEncoder || 
            !this._valueDecoder) 
        {
            throw new Error(`Cannot build CachedStorage without keyEncoder, keyDecoder, valueEncoder, valueDecoder`);
        }
        if (!this._grid) {
            throw new Error(`Cannot build CachedStorage without a given HamokGrid`);
        }
        if (this._generatedRequestTimeoutInMs === this._config.requestTimeoutInMs) {
            this._config.requestTimeoutInMs = this._grid.config.requestTimeoutInMs;
        }
        if (0 < this._config.neededResponse) {
            throw new Error(`CachedStorage must be built with 0 or undefined neededResponse config option as it must have response from every remote endpoints`)
        }

        const codec = new StorageCodec(
            createCodec(this._keyEncoder, this._keyDecoder),
            createCodec(this._valueEncoder, this._valueDecoder)
        );
        const responseChunker = (this._config.maxKeys < 1 && this._config.maxValues < 1)
            ? ResponseChunkerImpl.createPassiveChunker()
            : new ResponseChunkerImpl(this._config.maxKeys, this._config.maxValues)
        ;
        const storageId = this._config.storageId;
        const grid = this._grid;
        const sender = (message: Message) => {
            message.sourceId = grid.localEndpointId;
            message.storageId = storageId;
            message.protocol = MessageProtocol.STORAGE_COMMUNICATION_PROTOCOL;
            grid.transport.send(message);
        };
        const comlink = grid.createStorageComlink<K, V>()
            .setConfig(this._config)
            .setCodec(codec)
            .setResponseChunker(responseChunker)
            .setDefaultEndpointResolver(() => grid.remoteEndpointIds)
            .setNotificationSender(sender)
            .setRequestSender(sender)
            .setResponseSender(sender)
            .build();
        const result = new CachedStorage<K, V>(
            this._baseStorage,
            this._cache ?? new Map<K, V>(),
            comlink,
            this._config
        );
        grid.addStorageLink(result.id, comlink);
        return result;
    }
}