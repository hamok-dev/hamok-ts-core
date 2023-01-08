import { createCodec, Message, MessageProtocol, StorageCodec } from "@hamok-dev/common";
import { HamokGrid } from "../HamokGrid";
import { ResponseChunkerImpl } from "../messages/ResponseChunkerImpl";
import { SimpleStorage } from "../storages/SimpleStorage";
import { Storage } from "../storages/Storage";
import { StorageDecoder, StorageEncoder } from "../storages/StorageComlink";
import { SegmentedStorage, SegmentedStorageConfig } from "./SegmentedStorage";
import { v4 as uuid } from "uuid";

export class SegmentedStorageBuilder<K, V> {
    private readonly _generatedStorageId: string;
    private readonly _generatedRequestTimeoutInMs: number;
    private _config: SegmentedStorageConfig;
    private _grid?: HamokGrid;
    private _baseStorage?: Storage<K, V>;
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
            maxKeys: 0,
            maxValues: 0,
            storageId: this._generatedStorageId,
            ongoingRequestsSendingPeriodInMs: 0,
            maxRecursionRetry: 3,
            synchronize: {
                storageSync: true,
                clearEntries: true,
                getEntries: false,
                getKeys: false,
                getSize: false,
                deleteEntries: true,
                removeEntries: true,
                evictEntries: true,
                insertEntries: true,
                updateEntries: true
            }
        }
    }

    public get baseStorage(): Storage<K, V> | undefined {
        return this._baseStorage;
    }

    public setBaseStorage(storage?: Storage<K, V>): SegmentedStorageBuilder<K, V> {
        if (!storage) return this;
        this._baseStorage = storage;
        this._config.storageId = storage.id;
        return this;
    }

    public setHamokGrid(grid: HamokGrid): SegmentedStorageBuilder<K, V> {
        this._grid = grid;
        return this;
    }

    public withKeyEncoder(encoder: StorageEncoder<K>): SegmentedStorageBuilder<K, V> {
        this._keyEncoder = encoder;
        return this;
    }
    
    public withKeyDecoder(decoder: StorageDecoder<K>): SegmentedStorageBuilder<K, V> {
        this._keyDecoder = decoder;
        return this;
    }
    
    public withValueEncoder(encoder: StorageEncoder<V>): SegmentedStorageBuilder<K, V> {
        this._valueEncoder = encoder;
        return this;
    }
    
    public withValueDecoder(decoder: StorageDecoder<V>): SegmentedStorageBuilder<K, V> {
        this._valueDecoder = decoder
        return this;
    }

    public withConfig(partialConfig: Partial<SegmentedStorageConfig>): SegmentedStorageBuilder<K, V> {
        Object.assign(this._config, partialConfig);
        return this;
    }
    
    public build(): SegmentedStorage<K, V> {
        if (!this._baseStorage) {
            this._baseStorage = SimpleStorage.builder<K, V>()
                .setId(this._config.storageId)
                .build();
        } else {
            this._config.storageId = this._baseStorage.id;
        }
        if (this._config.storageId === this._generatedStorageId) {
            throw new Error(`Cannot build a SegmentedStorage without a given storageId`);
        }
        if (!this._keyEncoder || 
            !this._keyDecoder || 
            !this._valueEncoder || 
            !this._valueDecoder) 
        {
            throw new Error(`Cannot build SegmentedStorage without keyEncoder, keyDecoder, valueEncoder, valueDecoder`);
        }
        if (!this._grid) {
            throw new Error(`Cannot build SegmentedStorage without a given HamokGrid`);
        }
        if (this._generatedRequestTimeoutInMs === this._config.requestTimeoutInMs) {
            this._config.requestTimeoutInMs = this._grid.config.requestTimeoutInMs;
        }
        if (0 < this._config.neededResponse) {
            throw new Error(`SegmentedStorage must be built with 0 or undefined neededResponse config option as it must have response from every remote endpoints`)
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
        const result = new SegmentedStorage<K, V>(
            this._baseStorage,
            comlink,
            this._config
        );
        grid.addStorageLink(result.id, comlink);
        return result;
    }
}