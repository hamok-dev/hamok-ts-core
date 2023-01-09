import { createCodec, Message, MessageProtocol, MessageType, StorageCodec } from "@hamok-dev/common";
import { HamokGrid } from "../HamokGrid";
import { ResponseChunkerImpl } from "../messages/ResponseChunkerImpl";
import { SimpleStorage } from "../storages/SimpleStorage";
import { Storage } from "../storages/Storage";
import { StorageDecoder, StorageEncoder } from "../storages/StorageComlink";
import { ReplicatedStorage, ReplicatedStorageConfig } from "./ReplicatedStorage";
import { v4 as uuid } from "uuid";


export class ReplicatedStorageBuilder<K, V> {
    private readonly _generatedStorageId: string;
    private readonly _generatedRequestTimeoutInMs: number;
    private _config: ReplicatedStorageConfig;
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

    public setBaseStorage(storage?: Storage<K, V>): ReplicatedStorageBuilder<K, V> {
        if (!storage) return this;
        this._baseStorage = storage;
        this._config.storageId = storage.id;
        return this;
    }

    public setHamokGrid(grid: HamokGrid): ReplicatedStorageBuilder<K, V> {
        this._grid = grid;
        return this;
    }

    public withKeyEncoder(encoder: StorageEncoder<K>): ReplicatedStorageBuilder<K, V> {
        this._keyEncoder = encoder;
        return this;
    }
    
    public withKeyDecoder(decoder: StorageDecoder<K>): ReplicatedStorageBuilder<K, V> {
        this._keyDecoder = decoder;
        return this;
    }
    
    public withValueEncoder(encoder: StorageEncoder<V>): ReplicatedStorageBuilder<K, V> {
        this._valueEncoder = encoder;
        return this;
    }
    
    public withValueDecoder(decoder: StorageDecoder<V>): ReplicatedStorageBuilder<K, V> {
        this._valueDecoder = decoder
        return this;
    }

    public withConfig(partialConfig: Partial<ReplicatedStorageConfig>): ReplicatedStorageBuilder<K, V> {
        Object.assign(this._config, partialConfig);
        return this;
    }
    
    public build(): ReplicatedStorage<K, V> {
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
        // const sender = (message: Message) => {
        //     message.sourceId = grid.localEndpointId;
        //     message.storageId = storageId;
        //     message.protocol = MessageProtocol.STORAGE_COMMUNICATION_PROTOCOL;
        //     grid.transport.send(message);
        // };
        const setupAndGetType = (message: Message): MessageType | undefined => {
            message.sourceId = grid.localEndpointId;
            message.storageId = storageId;
            message.protocol = MessageProtocol.STORAGE_COMMUNICATION_PROTOCOL;
            return message.type;
        }
        const comlink = grid.createStorageComlink<K, V>()
            .setConfig(this._config)
            .setCodec(codec)
            .setResponseChunker(responseChunker)
            .setDefaultEndpointResolver(() => grid.remoteEndpointIds)
            .setNotificationSender(message => {
                setupAndGetType(message);
                grid.transport.send(message);
            })
            .setRequestSender(message => {
                const messageType = setupAndGetType(message);
                switch (messageType) {
                    case MessageType.CLEAR_ENTRIES_REQUEST:
                    case MessageType.INSERT_ENTRIES_REQUEST:
                    case MessageType.DELETE_ENTRIES_REQUEST:
                    case MessageType.UPDATE_ENTRIES_REQUEST:
                    case MessageType.REMOVE_ENTRIES_REQUEST:
                    case MessageType.EVICT_ENTRIES_REQUEST:
                        grid.submit(message);
                        break;
                    default:
                        grid.transport.send(message);
                        break;
                }
            })
            .setResponseSender(message => {
                setupAndGetType(message);
                grid.transport.send(message);
            })
            .build();
        const result = new ReplicatedStorage<K, V>(
            this._config,
            this._baseStorage,
            comlink,
        );
        grid.addStorageLink(result.id, comlink);
        return result;
    }
}