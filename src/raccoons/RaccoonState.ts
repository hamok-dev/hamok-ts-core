import { 
    EndpointStatesNotification, 
    HelloNotification, 
    Message, 
    RaftAppendEntriesRequestChunk, 
    RaftAppendEntriesResponse, 
    RaftVoteRequest, 
    RaftVoteResponse 
} from "@hamok-dev/common";
import { LogEntry } from "./LogEntry";
import { RaccoonBase, RaccoonConfig } from "./Raccoon";
import { RaftLogs } from "./RaftLogs";
import { RemotePeers } from "./RemotePeers";
import { SyncedProperties } from "./SyncedProperties";

export enum RaftState {
    NONE = "NONE",
    FOLLOWER = "FOLLOWER",
    CANDIDATE = "CANDIDATE",
    LEADER = "LEADER",
}

export abstract class RaccoonState {

    protected readonly base: RaccoonBase;
    protected constructor(base: RaccoonBase) {
        this.base = base;
    }

    protected get syncedProperties(): SyncedProperties {
        return this.base.props;
    }

    protected get config(): RaccoonConfig {
        return this.base.config;
    }

    protected get logs(): RaftLogs {
        return this.base.logs;
    }

    protected get remotePeers(): RemotePeers {
        return this.base.remotePeers;
    }

    protected get fullCommit(): boolean {
        return this.base.config.fullCommit;
    }

    protected getLocalPeerId(): string {
        return this.base.localPeerId;
    }

    protected get leaderId(): string | undefined {
        return this.base.leaderId;
    }

    public abstract get state(): RaftState;

    public abstract submit(message: Message): boolean;
    public abstract receiveHelloNotification(notification: HelloNotification): void;
    public abstract receiveVoteRequest(request: RaftVoteRequest): void;
    public abstract receiveVoteResponse(response: RaftVoteResponse): void;
    public abstract receiveEndpointStatesNotification(notification: EndpointStatesNotification): void;
    public abstract receiveAppendEntriesRequest(request: RaftAppendEntriesRequestChunk): void;
    public abstract receiveAppendEntriesResponse(response: RaftAppendEntriesResponse): void;
    public abstract start(): void;

    public abstract run(): void;

    protected sendAppendEntriesRequest(request: RaftAppendEntriesRequestChunk): void {
        this.base.ouboundEvents.emitOutboundRaftAppendEntriesRequestChunk(request);
    }

    protected sendAppendEntriesResponse(response: RaftAppendEntriesResponse): void {
        this.base.ouboundEvents.emitOutboundRaftAppendEntriesResponse(response);
    }

    protected sendVoteRequest(request: RaftVoteRequest): void {
        this.base.ouboundEvents.emitOutboundRaftVoteRequest(request);
    }

    protected sendVoteResponse(response: RaftVoteResponse): void {
        this.base.ouboundEvents.emitOutboundRaftVoteResponse(response);
    }

    protected sendEndpointStatesNotification(notification: EndpointStatesNotification): void {
        this.base.ouboundEvents.emitOutboundEndpointStatesNotification(notification);
    }

    protected sendHelloNotification(notification: HelloNotification): void {
        this.base.ouboundEvents.emitOutboundHelloNotification(notification);
    }

    protected follow(timedOutElection?: number): void {
        const suppliedTimedOutElection = Math.max(0, timedOutElection ?? 0);
        const successor = this.base.createFollowerState(suppliedTimedOutElection);
        this.base.changeState(successor);
    }

    protected lead(): void {
        const successor = this.base.createLeaderState();
        this.base.changeState(successor);
    }

    protected elect(timedOutElection: number): void {
        const successor = this.base.createCandidateState(timedOutElection);
        this.base.changeState(successor);
    }

    protected setActualLeaderId(value?: string): void {
        this.base.setActualLeader(value);
    }

    protected commitLogEntry(logEntry: LogEntry) {
        const message = logEntry.entry;
        // let's attach the commit index so the message holds that info for
        // upper layer possible usage
        // if (!message.raftCommitIndex) {
        //     message.raftCommitIndex = logEntry.index;
        // }
        this.base.commit(message);
    }

    protected requestStorageSync(): Promise<void> {
        return this.base.requestStorageSync();
    }
}