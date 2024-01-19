import { 
    Message,
    MessageProtocol,
    createLogger,
    SubmitMessageRequest,
    StorageSyncRequest,
    LogLevel,
    setLogLevel,
} from "@hamok-dev/common"
import { GridDispatcher } from "./GridDispatcher";
import { GridTransport, GridTransportAbstract } from "./GridTransport"
import { ChangedLeaderEvent, ChangedLeaderListener, EndpointIdListener, Raccoon, RaccoonConfig, RaccoonImpl } from "./raccoons/Raccoon";
import { StorageGridLink } from "./storages/StorageComlink";
import { CompletablePromise, CompletablePromiseState } from "./utils/CompletablePromise";
import { v4 as uuid } from "uuid";
import { LogEntry } from "./raccoons/LogEntry";
import { StorageComlinkBuilder } from "./storages/StorageComlinkBuilder";
import { SeparatedStorageBuilder } from "./distributedstorages/SeparatedStorageBuilder";
import { SeparatedStorage } from "./distributedstorages/SeparatedStorage";
import { Storage } from "./storages/Storage";
import { PubSubGridLink } from "./distributedevents/PubSubComlink";
import { SegmentedStorage } from "./distributedstorages/SegmentedStorage";
import { SegmentedStorageBuilder } from "./distributedstorages/SegmentedStorageBuilder";
import { CachedStorageBuilder } from "./distributedstorages/CachedStorageBuilder";
import { CachedStorage } from "./distributedstorages/CachedStorage";
import { PubSubComlinkBuilder } from "./distributedevents/PubSubComlinkBuilder";
import { PubSubBuilder } from "./distributedevents/PubSubBuilder";
import { PubSub } from "./distributedevents/PubSub";
import { ReplicatedStorage } from "./distributedstorages/ReplicatedStorage";
import { ReplicatedStorageBuilder } from "./distributedstorages/ReplicatesStorageBuilder";


const logger = createLogger("HamokGrid");

export interface StorageSyncResult {
    success: boolean,
    errors?: string[]
}

export interface PubSubSyncResult {
    success: boolean,
    errors?: string[]
}

type HamokGridConfig = {
    requestTimeoutInMs: number;
}

export type HamokGridBuilderConfig = HamokGridConfig & RaccoonConfig & {
    logLevel: LogLevel,
    raftLogExpirationTimeInMs: number;
}

export interface HamokGridBuilder {
    withRaftLogBaseMap(baseMap: Map<number, LogEntry>): HamokGridBuilder;
    withConfig(partialConfig: Partial<HamokGridBuilderConfig>): HamokGridBuilder;
    build(): HamokGrid;
}

export class HamokGrid {
    public static builder(): HamokGridBuilder {
        const config: HamokGridBuilderConfig = {
            requestTimeoutInMs: 3000,
            logLevel: 'warn',
            id: uuid(),
            electionTimeoutInMs: 1000,
            followerMaxIdleInMs: 1000,
            heartbeatInMs: 100,
            sendingHelloTimeoutInMs: 1000,
            peerMaxIdleTimeInMs: 5000,
            
            raftLogExpirationTimeInMs: 5 * 60 * 1000,

            fullCommit: false,
        };
        let raftLogBaseMap: Map<number, LogEntry> | undefined;
        
        const result = new class implements HamokGridBuilder {
            withConfig(partialConfig: Partial<HamokGridBuilderConfig>): HamokGridBuilder {
                Object.assign(config, partialConfig);
                return result;
            }
            withRaftLogBaseMap(baseMap: Map<number, LogEntry>): HamokGridBuilder {
                raftLogBaseMap = baseMap;
                return result;
            }
            build(): HamokGrid {
                const raccoonBuilder = RaccoonImpl.builder()
                    .setConfig(config);
                if (raftLogBaseMap) {
                    raccoonBuilder.setLogBaseMap(raftLogBaseMap);
                }
                raccoonBuilder.setLogExpirationTime(config.raftLogExpirationTimeInMs);
                
                setLogLevel(config.logLevel);

                return new HamokGrid(
                    raccoonBuilder.build(),
                    config
                );
                
            }

        }
        return result;
    }

    private readonly _dispatcher
    private _ongoingPromiseLeader?: Promise<string>;
    private _ongoingPromiseCommitSync?: Promise<number>;
    private _ongoingStorageSync?: Promise<boolean>;
    private _transport: GridTransportAbstract;
    private _raccoon: Raccoon;
    private _storageLinks = new Map<string, StorageGridLink>();
    private _pubsubLinks = new Map<string, PubSubGridLink>();
    public readonly config: HamokGridConfig;

    private constructor(
        raccoon: Raccoon,
        config: HamokGridConfig,
    ) {
        this.config = config;
        this._raccoon = raccoon;
        this._dispatcher = this._createGridDispatcher()
            .onStorageSyncRequest(request => {
                if (this.localEndpointId !== this.leaderId) {
                    return;
                }
                // const response = request.createResponse();
                // this._dispatcher.emitOutboundStorageSyncResponse(response);
                const response = request.createResponse(
                    this.localEndpointId,
                    this._raccoon.logs.size,
                    this._raccoon.logs.lastAppliedIndex,
                    this._raccoon.logs.commitIndex
                );
                this._dispatcher.sendStorageSyncResponse(response);
            })
            .onSubmitMessageRequest(request => {
                if (this.localEndpointId !== this.leaderId) {
                    if (this.leaderId && request.destinationEndpointId === this.localEndpointId) {
                        this._dispatcher.sendSubmitMessageResponse(request.createResponse(
                            false,
                            this.leaderId
                        ))
                    }
                    return;
                }
                const response = request.createResponse(
                    this._raccoon.submit(request.entry),
                    this.leaderId,
                );
                this._dispatcher.sendSubmitMessageResponse(response);
            });
            // .onSubmitMessageRequest(request => {
                
            // })
        this._transport = this._createTransport();
        this._raccoon.onOutboundMessage(message => {
            message.protocol = MessageProtocol.RAFT_COMMUNICATION_PROTOCOL;
            this._send(message);
        }).onStorageSyncRequested(promise => {
            logger.info("StorageSync is requested");
            this.sync(120 * 1000).catch(err => {
                logger.warn(`onStorageSyncRequested(): Synchronization failed`, err);
            })
            .then(() => promise.resolve())
            .catch(err => promise.reject(err));

        }).onChangedLeaderId(({ prevLeaderId, actualLeaderId }) => {
            if (actualLeaderId === undefined) {
                return;
            }
            logger.info(`onChangedLeaderId(): Leader is changed from ${prevLeaderId} to ${actualLeaderId}`);
            if (actualLeaderId !== this.localEndpointId) {
                this.sync(120 * 1000).catch(err => {
                    logger.warn(`onChangedLeaderId(): Synchronization failed`, err);
                }).then(() => {
                    logger.info(`onChangedLeaderId(): Sync is finished`);
                });
            }
        }).onCommittedEntry(message => {
            // console.warn(`Received committed message`, this.localEndpointId, message);
            // this._transport.receive(message);
            this._dispatch(message);
        })
    }

    public get transport(): GridTransport {
        return this._transport;
    }

    public get leaderId(): string | undefined {
        return this._raccoon.leaderId;
    }

    public get localEndpointId(): string {
        return this._raccoon.localPeerId;
    }

    public get remoteEndpointIds(): ReadonlySet<string> {
        return this._raccoon.remoteEndpointIds;
    }

    public start(): void {
        if (this._raccoon.started) {
            return;
        }
        this._raccoon.start();
    }

    public stop() {
        if (!this._raccoon.started) {
            return;
        }
        this._raccoon.stop();
    }

    public async promiseLeader(): Promise<string> {
        if (this._ongoingPromiseLeader) {
            return this._ongoingPromiseLeader;
        }
        this._ongoingPromiseLeader = new Promise<string>((resolve, reject) => {
            if (this.leaderId) {
                resolve(this.leaderId);
                return;
            }
            const listener = (event: ChangedLeaderEvent) => {
                if (!event.actualLeaderId) {
                    return;
                }
                this._raccoon.offChangedLeaderId(listener);
                resolve(event.actualLeaderId);
            };
            this._raccoon.onChangedLeaderId(listener);
        });
        this._ongoingPromiseLeader.finally(() => {
            this._ongoingPromiseLeader = undefined;
        })
        return this._ongoingPromiseLeader;
    }

    public async promiseCommitSync(promiseLeader = false, timeoutInMs?: number): Promise<number> {
        if (this._ongoingPromiseCommitSync) {
            return this._ongoingPromiseCommitSync;
        }
        if (!promiseLeader && !this._raccoon.leaderId) {
            return -1;
        }
        this._ongoingPromiseCommitSync = this.promiseLeader()
            .then(leaderId => this._dispatcher.requestStorageSync(new StorageSyncRequest(
                uuid(),
                leaderId,
                this.localEndpointId
            ))).then(storageSync => new Promise<number>((resolve, reject) => {
                if (storageSync.commitIndex === undefined) {
                    reject(`Cannot sync without commitIndex`);
                    return;
                }
                const responseCommitIndex = storageSync.commitIndex;
                const started = Date.now();
                const timer = setInterval(() => {
                    const actualCommitIndex = this._raccoon.logs.commitIndex;
                    if (actualCommitIndex < responseCommitIndex) {
                        if (timeoutInMs && timeoutInMs <= Date.now() - started) {
                            clearInterval(timer);
                            reject(`Timeout`);
                        }
                        return;
                    }
                    clearInterval(timer);
                    resolve(actualCommitIndex);
                }, 500);
            }));
        this._ongoingPromiseCommitSync.finally(() => {
            this._ongoingPromiseCommitSync = undefined;
        });
        return this._ongoingPromiseCommitSync;
    }

    public onRemoteEndpointJoined(listener: EndpointIdListener): this {
        this._raccoon.onJoinedRemotePeerId(listener);
        return this;
    }

    public offRemoteEndpointJoined(listener: EndpointIdListener): this {
        this._raccoon.offJoinedRemotePeerId(listener);
        return this;
    }

    public onRemoteEndpointDetached(listener: EndpointIdListener): this {
        this._raccoon.onDetachedRemotePeerId(listener);
        return this;
    }
    
    public offRemoteEndpointDetached(listener: EndpointIdListener): this {
        this._raccoon.offDetachedRemotePeerId(listener);
        return this;
    }

    public onLeaderChanged(listener: ChangedLeaderListener): this {
        this._raccoon.onChangedLeaderId(listener);
        return this;
    }

    public offLeaderChanged(listener: ChangedLeaderListener): this {
        this._raccoon.offChangedLeaderId(listener);
        return this;
    }

    public addRemoteEndpointId(remoteEndpointId: string): void {
        if (typeof remoteEndpointId !== "string") {
            throw new Error(`HamokGrid::addRemoteEndpointId(): Only string typed remote endpoint id can be added`);
        }
        this._raccoon.addRemotePeerId(remoteEndpointId);
    }

    public removeRemoteEndpointId(remoteEndpointId: string): void {
        if (typeof remoteEndpointId !== "string") {
            throw new Error(`HamokGrid::removeRemoteEndpointId(): Only string typed remote endpoint id can be added`);
        }
        this._raccoon.removeRemotePeerId(remoteEndpointId);
    }

    public createPubSubComlink(): PubSubComlinkBuilder {
        return new PubSubComlinkBuilder()
            .setHamokGrid(this);
    }

    public createStorageComlink<K, V>(): StorageComlinkBuilder<K, V> {
        return new StorageComlinkBuilder<K, V>()
            .setHamokGrid(this);
    }

    public createSeparatedStorage<K, V>(baseStorage?: Storage<K, V>): SeparatedStorageBuilder<K, V> {
        return SeparatedStorage.builder<K, V>()
            .setBaseStorage(baseStorage)
            .setHamokGrid(this);
    }

    public createSegmentedStorage<K, V>(baseStorage?: Storage<K, V>): SegmentedStorageBuilder<K, V> {
        return SegmentedStorage.builder<K, V>()
            .setBaseStorage(baseStorage)
            .setHamokGrid(this);
    }

    public createCachedStorage<K, V>(baseStorage?: Storage<K, V>): CachedStorageBuilder<K, V> {
        return CachedStorage.builder<K, V>()
            .setBaseStorage(baseStorage)
            .setHamokGrid(this);
    }

    public createReplicatedStorage<K, V>(baseStorage?: Storage<K, V>): ReplicatedStorageBuilder<K, V> {
        return ReplicatedStorage.builder<K, V>()
            .setBaseStorage(baseStorage)
            .setHamokGrid(this);
    }

    public createPubSub(): PubSubBuilder {
        return PubSub.builder()
            .setHamokGrid(this);
    }

    public addStorageLink(storageId: string, storageLink: StorageGridLink): void {
        this._storageLinks.set(storageId, storageLink);
    }

    public removeStorageLink(storageId: string): void {
        const comlink = this._storageLinks.get(storageId);
        if (!comlink) {
            return;
        }
        this._storageLinks.delete(storageId);
        comlink.close();
    }

    public addPubSubLink(pubSubLink: PubSubGridLink): void {
        this._pubsubLinks.set(pubSubLink.topic, pubSubLink);
    }

    public removePubSubLink(topic: string): void {
        this._pubsubLinks.delete(topic);
    }

    private _send(message: Message): void {
        message.sourceId = this._raccoon.localPeerId;
        // logger.info(`Sending message`, message);
        this._transport.send(message);
    }

    private _dispatch(message: Message) {
        // logger.info(`Received message`, message);
        switch(message.protocol) {
            case MessageProtocol.RAFT_COMMUNICATION_PROTOCOL:
                this._raccoon.dispatchInboundMessage(message);
                break;
            case undefined:
            case MessageProtocol.GRID_COMMUNICATION_PROTOCOL:
                this._dispatcher.receive(message);
                break;
            case MessageProtocol.STORAGE_COMMUNICATION_PROTOCOL:
                this._dispatchToStorage(message);
                break;
            case MessageProtocol.PUBSUB_COMMUNICATION_PROTOCOL:
                this._dispatchToPubSub(message);
                return;
        }
    }

    private _dispatchToStorage(message: Message) {
        if (message.storageId === undefined) {
            logger.warn(`_dispatchToStorage(): Cannot dispatch a message does not have a storageId`, message);
            return;
        }
        const storageLink = this._storageLinks.get(message.storageId);
        if (!storageLink) {
            logger.warn(`Cannot find a storage ${message.storageId} for message`, message);
            return;
        }
        storageLink.receive(message);
    }

    private _dispatchToPubSub(message: Message) {
        if (message.storageId === undefined) {
            logger.warn(`_dispatchToPubSub(): Cannot dispatch a message does not have a topic`, message);
            return;
        }
        const pubSubLink = this._pubsubLinks.get(message.storageId);
        if (!pubSubLink) {
            logger.warn(`Cannot find a PubSub ${message.storageId} for message`, message);
            return;
        }
        pubSubLink.receive(message);
    }

    private _createTransport(): GridTransportAbstract {
        /* eslint-disable @typescript-eslint/no-this-alias */
        const grid = this;
        const localEndpointId = grid.localEndpointId;
        return new class extends GridTransportAbstract {
            public receive(message: Message): void {
                if (message?.constructor !== Message) {
                    if (false === message instanceof Message) {
                        logger.warn(`receive(): Not message object received`);
                        return;
                    }
                }
                if (message.destinationId && message.destinationId !== localEndpointId) {
                    logger.warn(`Discarding message destination id (${message.destinationId}) is different from the local endpoint ${localEndpointId}`, message.type);
                    return;
                }
                grid._dispatch(message);
            }

            protected canSend(message: Message): boolean {
                // logger.info(`Can send message ${message.type} from ${message.sourceId} to ${message.destinationId}?`);
                if (message.destinationId === localEndpointId) {
                    // console.warn(`Sent it to myself`);
                    this.receive(message);
                    return false;
                }
                return true;
            }
        };
    }

    private _createGridDispatcher(): GridDispatcher {
        /* eslint-disable @typescript-eslint/no-this-alias */
        const grid = this;
        const result = new class extends GridDispatcher {
            protected send(message: Message): void {
                message.protocol = MessageProtocol.GRID_COMMUNICATION_PROTOCOL;
                grid._send(message);
            }
        }(
            this.config.requestTimeoutInMs,
        )
        return result;
    }


    public async submit(message: Message): Promise<boolean> {
        if (!this.leaderId) {
            return false;
        }
        if (this.localEndpointId === this.leaderId) {
            return this._raccoon.submit(message);
        }
        const request = new SubmitMessageRequest(
            uuid(),
            this.localEndpointId,
            message,
            this.leaderId,
        );
        const response = await this._dispatcher.requestSubmitMessage(request);
        return response.success;
    }

    public async sync(timeoutInMs?: number): Promise<boolean> {
        const result = new CompletablePromise<boolean>();
        let timer: ReturnType<typeof setTimeout> | undefined;
        if (timeoutInMs && 0 < timeoutInMs)  {
            timer = setTimeout(() => {
                if (result.state !== CompletablePromiseState.PENDING) {
                    return;
                }
                result.reject(new Error(`Timeout`));
            }, timeoutInMs);
        }
        if (!this._ongoingStorageSync) {
            this._ongoingStorageSync = this._dispatcher.requestStorageSync(new StorageSyncRequest(
                uuid(),
                this.leaderId,
                this.localEndpointId,
            )).then(storageSyncResponse => new Promise<boolean>((resolve) => {
                if (!storageSyncResponse || 
                    storageSyncResponse.commitIndex === undefined || 
                    storageSyncResponse.numberOfLogs === undefined
                ) {
                    logger.warn(`No commitIndex or numberOfLogs in storage sync response`, storageSyncResponse);
                    resolve(false);
                    return;
                }
                // console.warn("sync()", this.localEndpointId, this._raccoon.logs, storageSyncResponse, storageSyncResponse.commitIndex - storageSyncResponse.numberOfLogs <= this._raccoon.commitIndex);
                if (storageSyncResponse.commitIndex - storageSyncResponse.numberOfLogs <= this._raccoon.commitIndex) {
                    // the storage grid is in sync (theoretically)
                    logger.debug(`Sync ended, because it does not require total sync. 
                        The commitIndex is ${this._raccoon.commitIndex}, 
                        the leader commitIndex is ${storageSyncResponse.commitIndex} and the number of logs the 
                        leader has ${storageSyncResponse.numberOfLogs}, should be sufficient`
                    );
                    resolve(true);
                    return;
                }
                this._executeSync(storageSyncResponse.commitIndex, false)
                    .then(resolve)
                    .catch(err => {
                        logger.warn(`Failed sync`, err);
                        resolve(false);
                    });
            }));
            this._ongoingStorageSync.catch(err => {
                logger.warn("sync(): Error occurred while executing storage sync", err);
                return undefined;
            }).finally(() => {
                this._ongoingStorageSync = undefined;
            })
        }
        this._ongoingStorageSync.then((success) => {
            if (result.state !== CompletablePromiseState.PENDING) {
                return;
            }
            result.resolve(success);
        })
        .catch(err => result.reject(err))
        .finally(() => {
            if (timer) {
                clearTimeout(timer);
            }
        });
        return result;
    }


    private async _executeSync(newCommitIndex: number, setCommitIndexEvenIfSomethingFailed?: boolean): Promise<boolean> {
        const syncRequests: Promise<StorageSyncResult>[] = [];
        for (const storageLink of this._storageLinks.values()) {
            const promise = storageLink.requestStorageSync();
            syncRequests.push(promise);
        }
        
        const responses = syncRequests.length < 1 
            ? [] 
            : await Promise.all(syncRequests);

        let result = true;
        for (const { success } of responses) {
            result = result && success;
        }
        if (result || setCommitIndexEvenIfSomethingFailed) {
            this._raccoon.commitIndex = newCommitIndex;
        }
        return result;
    }

    public close(): void {
        this._raccoon.stop();
    }
}