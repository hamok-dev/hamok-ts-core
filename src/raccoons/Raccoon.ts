import { Message, createLogger  } from "@hamok-dev/common"
import EventEmitter from "events";
import { OutboundMessageListener } from "../GridDispatcher";
import { CompletablePromise } from "../utils/CompletablePromise";
import { CandidateState } from "./CandidateState";
import { FollowerState } from "./FollowerState";
import { LeaderState } from "./LeaderState";

import { LogEntry } from "./LogEntry";
import { RaccoonDispatcher } from "./RaccoonDispatcher";
import { RaccoonState, RaftState } from "./RaccoonState";
import { RaftLogs } from "./RaftLogs";
import { RemotePeers } from "./RemotePeers";
import { SyncedProperties } from "./SyncedProperties";
import { v4 as uuid } from "uuid";

const logger = createLogger(`RaccoonImpl`);

const JOINED_REMOTE_PEER_EVENT_NAME = "joinedRemotePeer";
const DETACHED_REMOTE_PEER_EVENT_NAME = "detachedRemotePeer";
const INACTIVE_REMOTE_PEER_EVENT_NAME = "inactiveRemotePeer";
const CHANGED_LEADER_ID_EVENT_NAME = "changedLeaderId";
const COMMITTED_ENTRY_EVENT_NAME = "committedEntry";
const STORAGE_SYNC_REQUESTED_EVENT_NAME = "storageSyncRequested";

export type ChangedLeaderEvent = {
    prevLeaderId?: string,
    actualLeaderId?: string,
}

export type StorageSyncRequestedListener = (completablePromise: CompletablePromise<void>) => void;
export type CommittedEntryListener = (message: Message) => void;
export type ChangedLeaderListener = (event: ChangedLeaderEvent) => void;
export type EndpointIdListener = (remoteEndpointId: string) => void;
export type DetachedRemoteEndpointListener = (remoteEndpointId: string) => void;

export interface Raccoon {
    readonly started: boolean;
    readonly logs: RaftLogs;
    readonly localPeerId: string;
    remoteEndpointIds: ReadonlySet<string>;
    commitIndex: number;
    leaderId?: string;

    onOutboundMessage(listener: OutboundMessageListener): Raccoon;
    offOutboundMessage(listener: OutboundMessageListener): Raccoon;
    dispatchInboundMessage(message: Message): void;

    onJoinedRemotePeerId(listener: EndpointIdListener): Raccoon;
    offJoinedRemotePeerId(listener: EndpointIdListener): Raccoon;

    onDetachedRemotePeerId(listener: EndpointIdListener): Raccoon;
    offDetachedRemotePeerId(listener: EndpointIdListener): Raccoon;

    onInactiveRemovePeerId(listener: EndpointIdListener): Raccoon;
    offInactiveRemovePeerId(listener: EndpointIdListener): Raccoon;

    onChangedLeaderId(listener: ChangedLeaderListener): Raccoon;
    offChangedLeaderId(listener: ChangedLeaderListener): Raccoon;

    onCommittedEntry(listener: CommittedEntryListener): Raccoon;
    offCommittedEntry(listener: CommittedEntryListener): Raccoon;

    onStorageSyncRequested(listener: StorageSyncRequestedListener): Raccoon;
    offStorageSyncRequested(listener: StorageSyncRequestedListener): Raccoon;
    
    submit(message: Message): boolean;

    addRemotePeerId(peerId: string): void;
    removeRemotePeerId(peerId: string): void;

    start(): void;
    stop(): void;
    
    // readonly numberOfCommits: number;
    // readonly lastApplied: number;
    
}

export interface RaccoonBase {
    readonly localPeerId: string;
    readonly config: RaccoonConfig;
    readonly props: SyncedProperties;
    readonly logs: RaftLogs;
    readonly leaderId?: string;
    readonly remotePeers: RemotePeers;
    readonly ouboundEvents: RaccoonDispatcher;
    requestStorageSync(): Promise<void>;
    changeState(successor?: RaccoonState): void;
    setActualLeader(value?: string): void
    commit(message: Message): void;

    createFollowerState(timedOutElection: number): FollowerState;
    createCandidateState(prevTimedOutElection: number): CandidateState;
    createLeaderState(): LeaderState;
}

export type RaccoonConfig = {
    id: string,
    electionTimeoutInMs: number;
    followerMaxIdleInMs: number,
    heartbeatInMs: number,
    sendingHelloTimeoutInMs: number,
    peerMaxIdleTimeInMs: number;
}

interface Builder {
    setConfig(config: Partial<RaccoonConfig>): Builder;
    setLogExpirationTime(valueInMs: number): Builder;
    setLogBaseMap(baseMap: Map<number, LogEntry>): Builder;
    build(): Raccoon;
}

export class RaccoonImpl implements Raccoon {
    public static builder(): Builder {
        let baseMap = new Map<number, LogEntry>();
        let logExpirationTimeInMs = 5 * 60 * 1000; // 5 mins
        const config: RaccoonConfig = {
            id: uuid(),
            electionTimeoutInMs: 1000,
            followerMaxIdleInMs: 1000,
            heartbeatInMs: 150,
            sendingHelloTimeoutInMs: 5000,
            peerMaxIdleTimeInMs: 10000,
        };
        const result: Builder = {
            setConfig: (partialConfig: Partial<RaccoonConfig>) => {
                Object.assign(config, partialConfig);
                return result;
            },
            setLogExpirationTime: (valueInMs: number) => {
                logExpirationTimeInMs = valueInMs;
                return result;
            },
            setLogBaseMap: (map: Map<number, LogEntry>) => {
                baseMap = map;
                return result;
            },
            build: () => {
                const raftLogs = new RaftLogs(baseMap, logExpirationTimeInMs);
                return new RaccoonImpl(
                    config,
                    raftLogs
                );
            }
        };
        return result;
    }
    
    private readonly _config: RaccoonConfig;
    private readonly _logs: RaftLogs;
    private readonly _remotePeers = new RemotePeers();
    private readonly _syncedProperties = new SyncedProperties();
    private readonly _emitter = new EventEmitter();
    private readonly _dispatcher = new RaccoonDispatcher();
    private _actualLeaderId?: string;
    private _state?: RaccoonState;
    private _timer?: ReturnType<typeof setInterval>;

    private constructor(
        config: RaccoonConfig,
        raftLogs: RaftLogs,
    ) {
        this._config = config;
        this._logs = raftLogs;
        this._dispatcher
            .onInboundHelloNotification(notification => {
                this._state?.receiveHelloNotification(notification);
            })
            .onInboundEndpointStatesNotification(notification => {
                this._state?.receiveEndpointStatesNotification(notification);
            })
            .onInboundRaftVoteRequest(request => {
                this._state?.receiveVoteRequest(request);
            })
            .onInboundRaftVoteResponse(response => {
                this._state?.receiveVoteResponse(response);
            })
            .onInboundRaftAppendEntriesRequestChunk(request => {
                this._state?.receiveAppendEntriesRequest(request);
            })
            .onInboundRaftAppendEntriesResponse(response => {
                this._state?.receiveAppendEntriesResponse(response);
            });
    }

    public onOutboundMessage(listener: OutboundMessageListener): this {
        this._dispatcher.onOutboundMessage(listener);
        // this._dispatcher.onOutboundMessage(message => {
        //     logger.warn(`${this.localPeerId} sent message`, message);
        // });
        return this;
    }

    public offOutboundMessage(listener: OutboundMessageListener): Raccoon {
        this._dispatcher.offOutboundMessage(listener);
        return this;
    }
    
    public onJoinedRemotePeerId(listener: EndpointIdListener): Raccoon {
        this._emitter.on(JOINED_REMOTE_PEER_EVENT_NAME, listener);
        return this;
    }

    public offJoinedRemotePeerId(listener: EndpointIdListener): Raccoon {
        this._emitter.off(JOINED_REMOTE_PEER_EVENT_NAME, listener);
        return this;
    }

    public onDetachedRemotePeerId(listener: EndpointIdListener): Raccoon {
        this._emitter.on(DETACHED_REMOTE_PEER_EVENT_NAME, listener);
        return this;
    }

    public offDetachedRemotePeerId(listener: EndpointIdListener): Raccoon {
        this._emitter.off(DETACHED_REMOTE_PEER_EVENT_NAME, listener);
        return this;
    }

    public onInactiveRemovePeerId(listener: EndpointIdListener): Raccoon {
        this._emitter.on(INACTIVE_REMOTE_PEER_EVENT_NAME, listener);
        return this;
    }

    public offInactiveRemovePeerId(listener: EndpointIdListener): Raccoon {
        this._emitter.off(INACTIVE_REMOTE_PEER_EVENT_NAME, listener);
        return this;
    }
    
    public onChangedLeaderId(listener: ChangedLeaderListener): Raccoon {
        this._emitter.on(CHANGED_LEADER_ID_EVENT_NAME, listener);
        return this;
    }

    public offChangedLeaderId(listener: ChangedLeaderListener): Raccoon {
        this._emitter.off(CHANGED_LEADER_ID_EVENT_NAME, listener);
        return this;
    }

    public onCommittedEntry(listener: CommittedEntryListener): Raccoon {
        this._emitter.on(COMMITTED_ENTRY_EVENT_NAME, listener);
        return this;
    }
    
    public offCommittedEntry(listener: CommittedEntryListener): Raccoon {
        this._emitter.off(COMMITTED_ENTRY_EVENT_NAME, listener);
        return this;
    }

    public onStorageSyncRequested(listener: StorageSyncRequestedListener): Raccoon {
        this._emitter.on(STORAGE_SYNC_REQUESTED_EVENT_NAME, listener);
        return this;
    }
    
    public offStorageSyncRequested(listener: StorageSyncRequestedListener): Raccoon {
        this._emitter.off(STORAGE_SYNC_REQUESTED_EVENT_NAME, listener);
        return this;
    }

    public get logs(): RaftLogs {
        return this._logs;
    }

    public get state(): RaftState {
        return this._state?.state ?? RaftState.NONE;
    }

    public get localPeerId(): string {
        return this._config.id;
    }

    public get leaderId(): string | undefined {
        return this._actualLeaderId;
    }

    public get commitIndex(): number {
        return this._logs.commitIndex;
    }

    public set commitIndex(value: number) {
        if (this._state === undefined) {
            logger.warn("Cannot set the commit index if there is no Raft State");
            return;
        }
        if (this._state.state !== RaftState.FOLLOWER) {
            logger.warn("Only Follower can sync with commit from API");
            return;
        }
        this._logs.reset(value);
        logger.info(`Logs are restarted to point to the application defined commit index ${this.commitIndex}. Every logs are purged`);
    }

    public get started(): boolean {
        return this._timer !== undefined;
    }

    public start(): void {
        if (this._timer) {
            logger.warn(`Attempted to start Raccoon twice`);
            return;
        }
        const base = this._createRaccoonBase();
        const followerState = new FollowerState(base, 0);
        base.changeState(followerState);
        this._timer = setInterval(() => {
            // logger.trace(`Call ${this._state?.state}.run()`);
            this._state?.run();
        }, this._config.heartbeatInMs);
    }

    public stop(): void {
        if (this._timer === undefined) {
            return;
        }
        clearInterval(this._timer);
        this._timer = undefined;
    }

    public get remoteEndpointIds(): ReadonlySet<string> {
        return this._remotePeers.getRemotePeerIds();
    }

    public dispatchInboundMessage(message: Message) {
        // logger.warn(`${this.localPeerId} received message`, message);
        this._dispatcher.dispatchInboundMessage(message);
    }
    
    public submit(message: Message): boolean {
        return this._state?.submit(message) ?? false;
    }

    public addRemotePeerId(peerId: string): void {
        this._remotePeers.join(peerId);
    }

    public removeRemotePeerId(peerId: string): void {
        this._remotePeers.detach(peerId);
    }

    private _createRaccoonBase(): RaccoonBase {
        const raccoon = this;
        const result = new class implements RaccoonBase {
            requestStorageSync(): Promise<void> {
                const completablePromise = new CompletablePromise<void>();
                raccoon._emitter.emit(STORAGE_SYNC_REQUESTED_EVENT_NAME, completablePromise);
                return completablePromise;
            }
            get localPeerId(): string {
                return raccoon._config.id;
            }
            get config(): RaccoonConfig {
                return raccoon._config;
            }
            get props(): SyncedProperties {
                return raccoon._syncedProperties;
            }
            get logs(): RaftLogs {
                return raccoon._logs;
            }
            get leaderId(): string | undefined {
                return raccoon._actualLeaderId;
            }
            get remotePeers(): RemotePeers {
                return raccoon._remotePeers;
            }
            get ouboundEvents(): RaccoonDispatcher {
                return raccoon._dispatcher;
            }
            changeState(successor?: RaccoonState): void {
                const predecessor = raccoon._state;
                raccoon._state = successor;
                successor?.start();
                logger.debug(`Changed state to from ${predecessor?.constructor.name} to ${successor?.constructor.name}`);
            }
            setActualLeader(value?: string): void {
                if (raccoon._actualLeaderId === value) {
                    return;
                }
                const prevLeaderId = raccoon._actualLeaderId;
                raccoon._actualLeaderId = value;
                const event: ChangedLeaderEvent = {
                    prevLeaderId,
                    actualLeaderId: value
                };
                raccoon._emitter.emit(CHANGED_LEADER_ID_EVENT_NAME, event);
            }
            commit(message: Message): void {
                raccoon._emitter.emit(COMMITTED_ENTRY_EVENT_NAME, message);
            }

            createCandidateState(prevTimedOutElection: number): CandidateState {
                return new CandidateState(
                    result,
                    prevTimedOutElection
                );
            }

            createFollowerState(timedOutElection: number): FollowerState {
                return new FollowerState(
                    result,
                    timedOutElection
                );
            }

            createLeaderState(): LeaderState {
                return new LeaderState(result);
            }
        }
        return result;
    }
}