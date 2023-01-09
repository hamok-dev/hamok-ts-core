import { Message, MessageProtocol } from "@hamok-dev/common";
import { HamokGrid } from "../HamokGrid";
import { PubSub, PubSubConfig } from "./PubSub";
import { PubSubComlinkConfig } from "./PubSubComlink";
import { v4 as uuid } from "uuid";


export class PubSubBuilder {
    private readonly _generatedTopic: string;
    private readonly _generatedRequestTimeoutInMs: number;
    private _config: PubSubComlinkConfig;
    private _grid?: HamokGrid;

    public constructor() {
        this._generatedTopic = uuid();
        this._generatedRequestTimeoutInMs = Math.random();
        this._config = {
            topic: this._generatedTopic,
            requestTimeoutInMs: this._generatedRequestTimeoutInMs,
            throwExceptionOnTimeout: true,
            neededResponse: 0,
        }
    }

    public setHamokGrid(grid: HamokGrid): PubSubBuilder {
        this._grid = grid;
        return this;
    }

    public withConfig(partialConfig: Partial<PubSubConfig>): PubSubBuilder {
        Object.assign(this._config, partialConfig);
        return this;
    }
    
    public build(): PubSub {
        if (!this._grid) {
            throw new Error(`Cannot build CachedStorage without a given HamokGrid`);
        }
        if (this._generatedRequestTimeoutInMs === this._config.requestTimeoutInMs) {
            this._config.requestTimeoutInMs = this._grid.config.requestTimeoutInMs;
        }
        if (0 < this._config.neededResponse) {
            throw new Error(`CachedStorage must be built with 0 or undefined neededResponse config option as it must have response from every remote endpoints`)
        }

        const topic = this._config.topic;
        const grid = this._grid;
        const sender = (message: Message) => {
            message.sourceId = grid.localEndpointId;
            message.storageId = topic;
            message.protocol = MessageProtocol.PUBSUB_COMMUNICATION_PROTOCOL;
            grid.transport.send(message);
        };
        const comlink = grid.createPubSubComlink()
            .setConfig(this._config)
            .setDefaultEndpointResolver(() => grid.remoteEndpointIds)
            .setNotificationSender(sender)
            .setRequestSender(sender)
            .setResponseSender(sender)
            .build();
        const result = new PubSub(
            comlink,
            this._config
        );
        grid.addPubSubLink(comlink);
        return result;
    }
}