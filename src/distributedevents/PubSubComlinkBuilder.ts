import { Message, PubSubCodec, StorageCodec } from "@hamok-dev/common";
import { v4 as uuid } from "uuid";
import { HamokGrid } from "../HamokGrid";
import { ResponseChunker } from "../messages/ResponseChunker";
import { PubSubComlink, PubSubComlinkConfig } from "./PubSubComlink";

export type MessageListener = (message: Message) => void;

export class PubSubComlinkBuilder {
    private readonly _generatedTopicId: string;
    private _config: PubSubComlinkConfig;
    private _grid?: HamokGrid;
    private _responseChunker?: ResponseChunker;
    private _requestSender?: MessageListener;
    private _responseSender?: MessageListener;
    private _notificationSender?: MessageListener;
    private _defaultEndpointResolver?: () => ReadonlySet<string>;
    
    public constructor() {
        this._generatedTopicId = uuid();
        this._config = {
            topic: this._generatedTopicId,
            throwExceptionOnTimeout: true,
            requestTimeoutInMs: 3000,
            neededResponse: 0,
        };
    }

    public get config(): PubSubComlinkConfig {
        return this._config;
    }

    public setConfig(partialConfig: Partial<PubSubComlinkConfig>): PubSubComlinkBuilder {
        Object.assign(this._config, partialConfig);
        return this;
    }

    public get requestSender(): MessageListener | undefined {
        return this._requestSender;
    }
    
    public setRequestSender(listener: MessageListener): PubSubComlinkBuilder {
        this._requestSender = listener;
        return this;
    }

    public get responseSender(): MessageListener | undefined {
        return this._responseSender;
    }

    public setResponseSender(listener: MessageListener): PubSubComlinkBuilder {
        this._responseSender = listener;
        return this;
    }

    public get notificationSender(): MessageListener | undefined {
        return this._notificationSender;
    }
    
    public setNotificationSender(listener: MessageListener): PubSubComlinkBuilder {
        this._notificationSender = listener;
        return this;
    }
    
    public setDefaultEndpointResolver(resolver: () => ReadonlySet<string>): PubSubComlinkBuilder {
        this._defaultEndpointResolver = resolver;
        return this;
    }

    public get responseChunker(): ResponseChunker | undefined {
        return this._responseChunker;
    }

    public setResponseChunker(responseChunker: ResponseChunker): PubSubComlinkBuilder {
        this._responseChunker = responseChunker;
        return this;
    }

    public get hamokGrid(): HamokGrid | undefined {
        return this._grid;
    }

    public setHamokGrid(grid: HamokGrid): PubSubComlinkBuilder {
        this._grid = grid;
        return this;
    }

    public build(): PubSubComlink {
        // console.warn(this);
        if (!this._defaultEndpointResolver) {
            throw new Error(`endpoint resolver must be provided`);
        }
        if (this._generatedTopicId === this._config.topic) {
            throw new Error(`topic must be set`);
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
        const result = new class extends PubSubComlink {
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
            new PubSubCodec()
        );

        return result;
    }
}
