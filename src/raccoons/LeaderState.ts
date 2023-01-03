import { 
    Message, 
    RaftAppendEntriesRequestChunk, 
    RaftAppendEntriesResponse, 
    RaftVoteRequest, 
    RaftVoteResponse, 
    EndpointStatesNotification, 
    HelloNotification, 
    createLogger,
    Collections
} from "@hamok-dev/common";
import { RaccoonBase } from "./Raccoon";
import { RaccoonState, RaftState } from "./RaccoonState";
import { v4 as uuid } from "uuid";

const logger = createLogger("LeaderState");

export class LeaderState extends RaccoonState {
    private _lastRemoteEndpointChecked = -1;
    
    private readonly _currentTerm: number;

    /**
     * leaders should track the sent index per peers. the reason behinf that is if
     * the response to the append request chunks arrives slower than the updateFollower is called,
     * then the same chunks is sent to follower making it slower to respond, making this leader sending
     * the same append request with the same entries more, making the follower even slower than before,
     * and the system explode. This tracking preventing to sending the same chunk of request twice
     * until the follower does not respond normally.
     */
    private _sentRequests = new Map<string, [string, number]>();
    private _unsyncedRemotePeers = new Set<string>();
    private _activeRemotePeerIds: ReadonlySet<string>;

    public constructor(
        base: RaccoonBase,
    ) {
        super(base);
        this.syncedProperties.currentTerm = this.syncedProperties.currentTerm + 1;
        this._currentTerm = this.syncedProperties.currentTerm;
        this._activeRemotePeerIds = new Set<string>();
    }

    public get state(): RaftState {
        return RaftState.LEADER;
    }

    public submit(message: Message): boolean {
        logger.trace(`Submitted entry started for message ${message}`);
        this.logs.submit(this._currentTerm, message);
        return true;
    }

    public receiveHelloNotification(notification: HelloNotification): void {
        const remotePeerId = notification.sourcePeerId;
        if (remotePeerId === undefined) {
            logger.warn(`Hello notification does not contain a source id`);
            return;
        } else if (remotePeerId === this.leaderId || remotePeerId === this.getLocalPeerId()) {
            // why I got a hello from myself?
            logger.warn("Got hello messages from the node itself");
            return;
        }

        // if we receive a hello notification from any peer we immediately respond with the endpoint state notification.
        const remotePeers = this.remotePeers;
        const changedBefore = remotePeers.changed;
        remotePeers.touch(remotePeerId);
        const changedAfter = remotePeers.changed;
        if (changedBefore == changedAfter) {
            // if nothing has changed we just acknoledge the hello
            this._sendEndpointStateNotification(new Set<string>(remotePeerId), this._activeRemotePeerIds);
            return;
        }
        this._updateActiveRemotePeerIds();

        // otherwise we send the new situation to all peers
        // and request the new peer to perform a sync
        this._sendEndpointStateNotification(remotePeers.getRemotePeerIds(), this._activeRemotePeerIds);
    }

    public receiveVoteRequest(request: RaftVoteRequest): void {
        // until this node is alive and have higher or equal term in append requests then anyone else,
        // it should vote false to any candidate request
        const response = request.createResponse(false);
        this.sendVoteResponse(response);
    }

    public receiveVoteResponse(response: RaftVoteResponse): void {
        const remotePeers = this.remotePeers;
        if (remotePeers.get(response.sourcePeerId) !== undefined) {
            // let's keep up to date the last touches
            remotePeers.touch(response.sourcePeerId);
        }
    }

    public receiveEndpointStatesNotification(notification: EndpointStatesNotification): void {
        logger.warn(`Leader received endpoint state notification from ${notification}.`);
    }

    public receiveAppendEntriesRequest(request: RaftAppendEntriesRequestChunk): void {
        if (request.term < this._currentTerm) {
            return;
        }
        if (request.leaderId === undefined) {
            logger.warn(`Append Request Chunk is received without leaderId ${request}`);
            return;

        }
        if (request.leaderId === this.getLocalPeerId()) {
            // loopback message?
            return;
        }
        if (this._currentTerm < request.term) {
            logger.warn(`Current term of the leader is 
                ${this._currentTerm}, and received Request Chunk ${request} have higher term.`);
            this.follow();
            return;
        }
        // terms are equal
        logger.warn(`Append Request Chunk is received from another leader in the same term. Selecting one leader in this case who has higher id. ${request}`);
        if (this.getLocalPeerId().localeCompare(request.leaderId) < 0) {
            // only one can remain!
            this.follow();
        }
    }

    public receiveAppendEntriesResponse(response: RaftAppendEntriesResponse): void {
        if (response.term < this._currentTerm) {
            // this response comes from a previous term, I should not apply it in any way
            return;
        }
        if (this._currentTerm < response.term) {
            // I am not the leader anymore, so it is best to go back to a follower state
            this.setActualLeaderId(undefined);
            this.follow();
            return;
        }
        // now we are talking in my term...
        logger.trace("Received RaftAppendEntriesResponse", response);
        const remotePeers = this.remotePeers;
        if (this.getLocalPeerId() !== response.sourcePeerId) {
            remotePeers.touch(response.sourcePeerId);
        }

        // processed means the remote peer processed all the chunks for the request
        if (!response.processed) {
            return;
        }

        // success means that the other end successfully accepted the request
        if (!response.success) {
            // having unsuccessful response, but proceeded all of the chunks
            // means we should or can send a request again if it was a complex one.
            this._sentRequests.delete(response.sourcePeerId);
            return;
        }
        const sourcePeerId = response.sourcePeerId;
        var sentRequest = this._sentRequests.delete(sourcePeerId);
        if (sentRequest === undefined) {
            // most likely a response to a keep alive or expired request
            return;
        }

        const logs = this.logs;
        const props = this.syncedProperties;
        const peerNextIndex = response.peerNextIndex;
        const remotePeerIds = remotePeers.getRemotePeerIds();

        props.nextIndex.set(sourcePeerId, peerNextIndex);
        props.matchIndex.set(sourcePeerId, peerNextIndex - 1);

        let maxCommitIndex = -1;
        for (const logEntry of logs) {
            if (peerNextIndex <= logEntry.index) {
                break;
            }
            // is this good here? so we will never commit things not created by our term?
            if (logEntry.term !== this._currentTerm) {
                continue;
            }
            let matchCount = 1;
            for (const peerId of remotePeerIds) {
                const matchIndex = props.matchIndex.get(peerId) ?? -1;
                if (logEntry.index <= matchIndex) {
                    ++matchCount;
                }
            }
            logger.trace(`
                logIndex: ${logEntry.index}, 
                matchCount: ${matchCount}, 
                remotePeerIds: ${remotePeerIds.size} 
                commit: ${remotePeerIds.size + 1 < matchCount * 2}`
            );
            if (remotePeerIds.size + 1 < matchCount * 2) {
                maxCommitIndex = Math.max(maxCommitIndex, logEntry.index);
            }
        }
        if (0 <= maxCommitIndex) {
            logger.debug(`Committing index until ${maxCommitIndex} at leader state`);
            for (const committedLogEnty of logs.commitUntil(maxCommitIndex)) {
                this.commitLogEntry(committedLogEnty);
            }
        }
    }
    
    public start(): void {
        this.setActualLeaderId(this.getLocalPeerId());
        this._updateFollowers();
    }

    public run(): void {
        this._updateFollowers();
        if (this._updateActiveRemotePeerIds()) {
            const remotePeers = this.remotePeers;
            this._sendEndpointStateNotification(remotePeers.getRemotePeerIds(), this._activeRemotePeerIds);
        }
        if (this.remotePeers.size < 1) {
            logger.warn("Leader endpoint should become a follower because no remote endpoint is available");
            this.follow();
        }
    }

    private _updateActiveRemotePeerIds(): boolean {
        const config = this.config;
        const now = Date.now();
        if (this._lastRemoteEndpointChecked < 0) {
            this._lastRemoteEndpointChecked = now;
            return false;
        } else if (now - this._lastRemoteEndpointChecked < config.peerMaxIdleTimeInMs) {
            return false;
        }
        this._lastRemoteEndpointChecked = now;

        const currentActiveRemotePeerIds = this._activeRemotePeerIds;
        const remotePeers = this.remotePeers;
        if (config.peerMaxIdleTimeInMs < 1) {
            this._activeRemotePeerIds = remotePeers.getRemotePeerIds();
            return !Collections.equalSets(currentActiveRemotePeerIds, this._activeRemotePeerIds);
        }

        const newActiveRemotePeerIds = new Set<string>();
        for (const remotePeer of remotePeers) {
            if (config.peerMaxIdleTimeInMs < now - remotePeer.touched) {
                continue;
            }
            newActiveRemotePeerIds.add(remotePeer.id);
        }
        this._activeRemotePeerIds = newActiveRemotePeerIds;
        return !Collections.equalSets(currentActiveRemotePeerIds, this._activeRemotePeerIds);
    }

    private _updateFollowers() {
        const config = this.config;
        const props = this.syncedProperties;
        const logs = this.logs;
        const remotePeers = this.remotePeers;
        const now = Date.now();
        for (const remotePeer of remotePeers) {
            const peerId = remotePeer.id;
            const peerNextIndex = props.nextIndex.get(peerId) ?? 0;
            const prevLogIndex = peerNextIndex - 1;
            let prevLogTerm = -1;
            if (0 <= prevLogIndex) {
                const logEntry = logs.get(prevLogIndex);
                if (logEntry != null) {
                    prevLogTerm = logEntry.term;
                }
            }

            const entries = logs.collectEntries(peerNextIndex);
            if (peerNextIndex < logs.lastAppliedIndex) {
                if (this._unsyncedRemotePeers.add(peerId)) {
                    logger.warn(`Collected ${entries.length} entries, but peer ${peerId} should need ${logs.nextIndex - peerNextIndex}. The peer should request a commit sync`);
                }
            } else if (0 < this._unsyncedRemotePeers.size) {
                this._unsyncedRemotePeers.delete(peerId);
            }
            let sentRequest = this._sentRequests.get(peerId);
            if (sentRequest !== undefined) {
                const [requestId, requestCreated] = sentRequest;
                // we kill the sent request if it is older than the threshold
                if (requestCreated < now - 30000) {
                    this._sentRequests.delete(peerId);
                    sentRequest = undefined;
                }
            }
            const requestId = uuid();
            // we should only sent an entryfull request if the remote peer does not have one, and we have something to add
            if (sentRequest === undefined && entries !== undefined && 0 < entries.length) {
                for (let sequence = 0; sequence < entries.length; ++sequence) {
                    const entry = entries[sequence];
                    const request = new RaftAppendEntriesRequestChunk(
                            requestId,
                            peerId,
                            this.getLocalPeerId(),
                            logs.commitIndex,
                            logs.nextIndex,
                            prevLogIndex,
                            prevLogTerm,
                            this._currentTerm,
                            sequence,
                            sequence == entries.length - 1,
                            entry,
                    );
//                    logger.info("Sending", request);
                    this.sendAppendEntriesRequest(request);
                }
                sentRequest = [requestId, now];
                this._sentRequests.set(peerId, sentRequest);
            } else { // no entries
                const appendEntries = new RaftAppendEntriesRequestChunk(
                    requestId,
                    peerId,
                    this.getLocalPeerId(),
                    logs.commitIndex,
                    logs.nextIndex,
                    prevLogIndex,
                    prevLogTerm,
                    this._currentTerm,
                    0, // sequence
                    true, // last message
                    undefined, // entry
                );
                this.sendAppendEntriesRequest(appendEntries);
            }
        }
    }

    private _sendEndpointStateNotification(targetRemotePeerIds: ReadonlySet<string>, activeRemoteEndpointIds: ReadonlySet<string>) {
        const props = this.syncedProperties;
        const logs = this.logs;
        for (const remotePeerId of targetRemotePeerIds) {
            const notification = new EndpointStatesNotification(
                this.getLocalPeerId(),
                remotePeerId,
                this._currentTerm,
                logs.commitIndex,
                logs.nextIndex,
                logs.size,
                activeRemoteEndpointIds,
            );
            this.sendEndpointStatesNotification(notification);
        }
    }
}