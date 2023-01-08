import { Message, StorageCodec } from "@hamok-dev/common";
import { StorageComlink, StorageComlinkConfig } from "./StorageComlink";
import { v4 as uuid } from "uuid";
import { HamokGrid } from "../HamokGrid";
import { ResponseChunker } from "../messages/ResponseChunker";

export type MessageListener = (message: Message) => void;
type OnBuiltCallback<K, V> = (comlink: StorageComlink<K, V>) => void;

export class StorageComlinkBuilder<K, V> {
    private readonly _generatedStorageId: string;
    private _onBuiltCallbacks: OnBuiltCallback<K, V>[] = [];
    private _config: StorageComlinkConfig;
    private _codec?: StorageCodec<K, V>;
    private _grid?: HamokGrid;
    private _responseChunker?: ResponseChunker;
    private _requestSender?: MessageListener;
    private _responseSender?: MessageListener;
    private _notificationSender?: MessageListener;
    private _defaultEndpointResolver?: () => ReadonlySet<string>;
    
    public constructor() {
        this._generatedStorageId = uuid();
        this._config = {
            storageId: this._generatedStorageId,
            throwExceptionOnTimeout: true,
            requestTimeoutInMs: 3000,
            neededResponse: 0,
            ongoingRequestsSendingPeriodInMs: 1000,
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
            }
        };
    }

    public get config(): StorageComlinkConfig {
        return this._config;
    }

    public setConfig(partialConfig: Partial<StorageComlinkConfig>): StorageComlinkBuilder<K, V> {
        Object.assign(this._config, partialConfig);
        return this;
    }

    public get requestSender(): MessageListener | undefined {
        return this._requestSender;
    }
    
    public setRequestSender(listener: MessageListener): StorageComlinkBuilder<K, V> {
        this._requestSender = listener;
        return this;
    }

    public get responseSender(): MessageListener | undefined {
        return this._responseSender;
    }

    public setResponseSender(listener: MessageListener): StorageComlinkBuilder<K, V>{
        this._responseSender = listener;
        return this;
    }

    public get notificationSender(): MessageListener | undefined {
        return this._notificationSender;
    }
    
    public setNotificationSender(listener: MessageListener): StorageComlinkBuilder<K, V>{
        this._notificationSender = listener;
        return this;
    }
    
    public setDefaultEndpointResolver(resolver: () => ReadonlySet<string>): StorageComlinkBuilder<K, V> {
        this._defaultEndpointResolver = resolver;
        return this;
    }

    public get responseChunker(): ResponseChunker | undefined {
        return this._responseChunker;
    }

    public setResponseChunker(responseChunker: ResponseChunker): StorageComlinkBuilder<K, V>{
        this._responseChunker = responseChunker;
        return this;
    }

    public get hamokGrid(): HamokGrid | undefined {
        return this._grid;
    }

    public setHamokGrid(grid: HamokGrid): StorageComlinkBuilder<K, V>{
        this._grid = grid;
        return this;
    }

    public get codec(): StorageCodec<K, V> | undefined {
        return this._codec;
    }

    public setCodec(codec: StorageCodec<K, V>): StorageComlinkBuilder<K, V> {
        this._codec = codec;
        return this;
    }

    public onBuilt(comlink: OnBuiltCallback<K, V>): StorageComlinkBuilder<K, V> {
        this._onBuiltCallbacks.push(comlink);
        return this;
    }

    public build(): StorageComlink<K, V> {
        // console.warn(this);
        if (!this._codec) {
            throw new Error(`Codec must be provided to build a comlink`);
        }
        if (!this._defaultEndpointResolver) {
            throw new Error(`endpoint resolver must be provided`);
        }
        if (this._generatedStorageId === this._config.storageId) {
            throw new Error(`storageId must be set`);
        }
        if (!this._notificationSender) {
            throw new Error(`Notification sender must be set`);
        }
        if (!this._responseSender) {
            throw new Error(`Request sender must be set`);
        }
        if (!this._requestSender) {
            throw new Error(`Response sender must be set`);
        }
        if (!this._responseChunker) {
            throw new Error(`Response chunker must be set`);
        }
        if (!this._grid) {
            throw new Error(`HamokGrid must be set`);
        }

        const defaultEndpointResolver = this._defaultEndpointResolver;
        const notificationSender = this._notificationSender;
        const requestSender = this._requestSender;
        const responseSender = this._responseSender;
        const result = new class extends StorageComlink<K, V> {
            protected defaultResolvingEndpointIds(): ReadonlySet<string> {
                return defaultEndpointResolver();
            }
            protected sendNotification(message: Message): void {
                notificationSender(message);
            }
            protected sendRequest(message: Message): void {
                requestSender(message);
            }
            protected sendResponse(message: Message): void {
                responseSender(message);
            }
        }(
            this._config,
            this._grid,
            this._responseChunker,
            this._codec
        );

        for(const callback of this._onBuiltCallbacks) {
            callback(result);
        }
        return result;
    }
}
