import { 
    Message,
    MessageProtocol,
    createLogger,
    SubmitMessageRequest,
    StorageSyncRequest,
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


const logger = createLogger("HamokGrid");

export interface StorageSyncResult {
    success: boolean,
    errors?: string[]
}

export interface PubSubSyncResult {
    success: boolean,
    errors?: string[]
}

export type HamokGridConfig = {
    requestTimeoutInMs: number;
}

type HamokGridBuilderConfig = HamokGridConfig & RaccoonConfig & {
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
            id: uuid(),
            electionTimeoutInMs: 1000,
            followerMaxIdleInMs: 1000,
            heartbeatInMs: 100,
            sendingHelloTimeoutInMs: 1000,
            peerMaxIdleTimeInMs: 5000,
            
            raftLogExpirationTimeInMs: 5 * 60 * 1000,
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
                return new HamokGrid(
                    raccoonBuilder.build(),
                    config
                );
            }

        }
        return result;
    }

    private readonly _dispatcher
    private _ongoingStart?: Promise<void>;
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
        this._transport = this._createTransport();
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
            })
            // .onSubmitMessageRequest(request => {
                
            // })
        this._raccoon.onOutboundMessage(message => {
            message.protocol = MessageProtocol.RAFT_COMMUNICATION_PROTOCOL;
            this._send(message);
        }).onStorageSyncRequested(promise => {
            this.sync(120 * 1000).catch(err => {
                logger.warn(`onStorageSyncRequested(): Synchronization failed`, err);
            });
        }).onChangedLeaderId(({ actualLeaderId }) => {
            if (actualLeaderId === undefined) {
                return;
            }
            this.sync(120 * 1000).catch(err => {
                logger.warn(`onChangedLeaderId(): Synchronization failed`, err);
            });
        }).onCommittedEntry(message => {
            this._transport.receive(message);
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

    public start(): Promise<void> {
        if (this._ongoingStart) {
            return this._ongoingStart;
        }
        this._ongoingStart = new Promise((resolve, reject) => {
            if (!this._raccoon.started) {
                this._raccoon.start();
            }
            if (this.leaderId) {
                resolve();
                return;
            }
            const listener = (event: ChangedLeaderEvent) => {
                if (!event.actualLeaderId) {
                    return;
                }
                this._raccoon.offChangedLeaderId(listener);
                resolve();
            };
            this._raccoon.onChangedLeaderId(listener);
        });
        this._ongoingStart.finally(() => {
            this._ongoingStart = undefined;
        })
        return this._ongoingStart;
    }

    public stop() {
        if (!this._raccoon.started) {
            return;
        }
        this._raccoon.stop();
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
        this._raccoon.addRemotePeerId(remoteEndpointId);
    }

    public removeRemoteEndpointId(remoteEndpointId: string): void {
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

    public addStorageLink(storageId: string, storageLink: StorageGridLink): void {
        this._storageLinks.set(storageId, storageLink);
    }

    public removeStorageLink(storageId: string): void {
        this._storageLinks.delete(storageId);
    }

    public addPubSubLink(pubSubLink: PubSubGridLink): void {
        this._pubsubLinks.set(pubSubLink.topic, pubSubLink);
    }

    public removePubSubLink(topic: string): void {
        this._pubsubLinks.delete(topic);
    }

    private _send(message: Message): void {
        message.sourceId = this._raccoon.localPeerId;
        this._transport.send(message);
    }

    private _createTransport(): GridTransportAbstract {
        const gridDispatcher = this._dispatcher;
        const raccoon = this._raccoon;
        const storageLinks = this._storageLinks;
        const pubSubLinks = this._pubsubLinks;
        return new class extends GridTransportAbstract {
            public receive(message: Message): void {
                switch(message.protocol) {
                    case undefined:
                    case MessageProtocol.RAFT_COMMUNICATION_PROTOCOL:
                        raccoon.dispatchInboundMessage(message);
                        break;
                    case undefined:
                    case MessageProtocol.GRID_COMMUNICATION_PROTOCOL:
                        gridDispatcher.receive(message);
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
                const storageLink = storageLinks.get(message.storageId);
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
                const pubSubLink = pubSubLinks.get(message.storageId);
                if (!pubSubLink) {
                    logger.warn(`Cannot find a PubSub ${message.storageId} for message`, message);
                    return;
                }
                pubSubLink.receive(message);
            }
        };
    }

    private _createGridDispatcher(): GridDispatcher {
        const grid = this;
        const result = new class extends GridDispatcher {
            protected send(message: Message): void {
                message.protocol = MessageProtocol.STORAGE_COMMUNICATION_PROTOCOL;
                grid._send(message);
            }
        }
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
            })
        }
        if (!this._ongoingStorageSync) {
            this._ongoingStorageSync = new Promise<boolean>(async (resolve, reject) => {
                const storageSyncResponse = await this._dispatcher.requestStorageSync(new StorageSyncRequest(
                    uuid(),
                    this.leaderId,
                    this.localEndpointId,
                )).catch(err => {
                    logger.warn("sync(): Error occurred while executing storage sync", err);
                    return undefined;
                });
                if (!storageSyncResponse || storageSyncResponse.commitIndex === undefined || !storageSyncResponse.numberOfLogs) {
                    logger.warn(`No commitIndex or numberOfLogs in storage sync response`, storageSyncResponse);
                    resolve(false);
                    return;
                }
                if (storageSyncResponse.commitIndex - storageSyncResponse.numberOfLogs <= this._raccoon.commitIndex) {
                    // the storage grid is in sync (theoretically)
                    logger.info(`Sync ended, because it does not require total sync. 
                        The commitIndex is ${this._raccoon.commitIndex}, 
                        the leader commitIndex is ${storageSyncResponse.commitIndex} and the number of logs the 
                        leader has ${storageSyncResponse.numberOfLogs}, should be sufficient`
                    );
        
                    return true;
                }
                this._executeSync(storageSyncResponse.commitIndex, false)
                    .then(resolve)
                    .catch(err => {
                        logger.warn(`Failed sync`, err);
                        resolve(false);
                    });
            });
            this._ongoingStorageSync.finally(() => {
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