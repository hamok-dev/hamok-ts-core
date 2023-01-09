import { 
    Message, 
    RaftAppendEntriesRequestChunk, 
    RaftAppendEntriesResponse, 
    RaftVoteRequest, 
    RaftVoteResponse, 
    EndpointStatesNotification, 
    HelloNotification, 
    createLogger,
} from "@hamok-dev/common";
import { RaccoonBase } from "./Raccoon";
import { RaccoonState, RaftState } from "./RaccoonState";
import { RaftAppendEntriesRequest } from "./RaftAppendEntriesRequest";

const logger = createLogger("FollowerState");

export class FollowerState extends RaccoonState {
    
    private _updated = Date.now();
    private _sentHello = 0;
    private _pendingRequests = new Map<string, RaftAppendEntriesRequest>();
    private _syncRequested = false;
    private _timedOutElection: number;
    private _extraWaitingTime = 0;
    private _receivedEndpointNotification = false;
    private _shouldLogOnceFlags = 0;
    private _unsyncRaftLogsLogged = false;

    public constructor(base: RaccoonBase, timedOutElection: number) {
        super(base);
        this._timedOutElection = timedOutElection;
        if (0 < timedOutElection) {
            const config = this.base.config;
            let extraWaitingTimeInMs = (Math.random() * 10 + 1) * config.electionTimeoutInMs;
            for (let i = 0; i < Math.min(30, timedOutElection); ++i) {
                const multiplier = Math.random() * 9 + 1;
                extraWaitingTimeInMs += (Math.random() + 1) * multiplier;
                this._extraWaitingTime = extraWaitingTimeInMs;
            }
        }
        const props = this.syncedProperties;
        props.votedFor = undefined;
        this.setActualLeaderId(undefined);
    }

    public start() {

    }

    public submit(message: Message): boolean {
        logger.debug(`${this.state} got a submit for message ${message}`);
        return false;
    }

    public receiveHelloNotification(notification: HelloNotification): void {
        logger.trace("{} received hello notification {}", this.getLocalPeerId(), notification);
        // hello can be a broadcasted hello from a remote endpoint
        // or a hello response for our hello
        if (!notification.destinationPeerId) {
            // so this is a broadcasted hello.
            if (notification.raftLeaderId || !this.leaderId) {
                // if the remote endpoint know who is the leader or we don't we do not respond.
                return;    
            }
            this.sendHelloNotification(new HelloNotification(
                this.getLocalPeerId(),
                notification.sourcePeerId,
                this.leaderId
            ));
            return;
        }
        if (notification.destinationPeerId === this.getLocalPeerId()) {
            // response to our hello broadcast
            if (!notification.raftLeaderId || this.leaderId) {
                // if we have our leader id or the response does not this hello is pointless
                return;
            }
            if (this.remotePeers.get(notification.raftLeaderId) === undefined) {
                // so there is a leader in the grid, but we don't know about it
                // it's better not to start an election just becasue we 
                // don't have enough information
                this._updated = Date.now();
            }
        }
    }

    public receiveEndpointStatesNotification(notification: EndpointStatesNotification): void {
        // update the server endpoint states
        const props = this.syncedProperties;
        if (notification.term < props.currentTerm) {
            logger.warn(`Received endpoint state notification from a node 
                (${notification.sourceEndpointId}) has lower term than this 
                (remoteTerm: ${notification.term}, 
                this term: ${props.currentTerm}). The reporting node should go into a follower mode`
            );
            return;
        }
        this._receivedEndpointNotification = true;
        if (this.leaderId === undefined) {
            this.setActualLeaderId(notification.sourceEndpointId);
        }
        // TODO: compare our active endpoints with the remote one and emit events accordingly

        this._updated = Date.now();
    }
    
    public receiveVoteRequest(request: RaftVoteRequest): void {
        const props = this.syncedProperties;
        logger.trace("{} received a vote request {}, votedFor: {}", this.getLocalPeerId(), request, props.votedFor);
        if (request.term <= props.currentTerm) {
            // someone requested a vote from a previous or equal term.
            this.sendVoteResponse(request.createResponse(false));
            return;
        }
        const logs = this.logs;
        
        if (0 < logs.commitIndex) {
            // if this follower has committed logs!
            if (request.lastLogIndex < logs.commitIndex) {
                // if the highest index of the candidate is smaller than the commit index of this,
                // then that candidate should not lead this cluster, and wait for another leader who can
                this.sendVoteResponse(request.createResponse(false));
                return;
            }    
        }
        
        let voteGranted = props.votedFor === undefined;
        if (voteGranted) {
            // when we vote for this candidate for the first time,
            // let's restart the timer, so we wait a bit more to give time to win the election before we run for presidency.
            props.votedFor = request.candidateId;
            this._updated = Date.now();
        } else {
            // maybe we already voted for the candidate itself?
            voteGranted = props.votedFor === request.candidateId;
        }
        var response = request.createResponse(voteGranted);
        logger.info(`${this.getLocalPeerId()} send a vote response ${response}.`);
        this.sendVoteResponse(response);
    }
    
    public receiveVoteResponse(response: RaftVoteResponse): void {
        const remotePeers = this.remotePeers;
        if (response.sourcePeerId !== undefined && remotePeers.get(response.sourcePeerId) !== undefined) {
            // let's keep up to date the last touches
            remotePeers.touch(response.sourcePeerId);
        }
    }
    
    public receiveAppendEntriesRequest(requestChunk: RaftAppendEntriesRequestChunk): void {
        const props = this.syncedProperties;
        let currentTerm = props.currentTerm;
        if (requestChunk.term < currentTerm) {
            logger.warn(`Append entries request appeared from a previous term. currentTerm: ${currentTerm}, received entries request term: ${requestChunk.term}`);
            var response = requestChunk.createResponse(false, -1, false);
            this.sendAppendEntriesResponse(response);
            return;
        }

        if (currentTerm < requestChunk.term) {
            logger.info(`Term for follower has been changed from ${currentTerm} to ${requestChunk.term}`);
            currentTerm = requestChunk.term;
            props.currentTerm = currentTerm;
            props.votedFor = undefined;
        }
        // let's restart the timer
        this._updated = Date.now();

        // and make sure next election we don't add unnecessary offset
        this._timedOutElection = 0;

         // set the actual leader
         if (requestChunk.leaderId !== undefined) {
            this.setActualLeaderId(requestChunk.leaderId);
        }

        // let's touch the leader (wierd sentence and I don't want to elaborate)
        if (this.getLocalPeerId() !== requestChunk.leaderId) {
            this.remotePeers.touch(requestChunk.leaderId);
        }

        const logs = this.logs;
        if (requestChunk.entry === undefined && requestChunk.sequence === 0) {
            if (requestChunk.lastMessage === false) {
                logger.warn(`Entries cannot be null if it is a part of chunks and thats not the last message`);
                var response = requestChunk.createResponse(false, -1, true);
                this.sendAppendEntriesResponse(response);
                return;
            }
            // that was a keep alive message
            if (!this._syncRequested) {
                // try to update if a sync is not in progress
                this.updateCommitIndex(requestChunk.leaderCommit);
            }
            var response = requestChunk.createResponse(true, logs.nextIndex, true);
            this.sendAppendEntriesResponse(response);
            return;
        }

        // assemble here
        let request = this._pendingRequests.get(requestChunk.requestId);
        if (request === undefined) {
            request = new RaftAppendEntriesRequest(requestChunk.requestId);
            this._pendingRequests.set(requestChunk.requestId, request);
        }
        request.add(requestChunk);
        if (request.ready === false) {
            var response = requestChunk.createResponse(true, -1, false);
            this.sendAppendEntriesResponse(response);
            return;
        }
        this._pendingRequests.delete(requestChunk.requestId);

        logger.trace(`Received RaftAppendEntriesRequest ${request} Entries: ${request.entries?.length}`);
        if (this._syncRequested) {
            if ((this._shouldLogOnceFlags & 1) == 0) {
                logger.warn("Commit sync is being executed at the moment");
                this._shouldLogOnceFlags += 1;
            }

            // until we do not sync we cannot process and go forward with our index
            var response = requestChunk.createResponse(
                    true,
                    logs.nextIndex,
                    true
            );
            this.sendAppendEntriesResponse(response);
            return;
        } else if ((this._shouldLogOnceFlags & 1) == 1) {
            this._shouldLogOnceFlags -= 1;
        }

        if (logs.nextIndex < request.leaderNextIndex - (request.entries?.length ?? 0)) {
            if (!this._syncRequested) {
                logger.warn(`The next index is 
                    ${logs.nextIndex}, and the leader index is: 
                    ${request.leaderNextIndex}, the provided entries are: 
                    ${request.entries?.length}. It is insufficient to close the gap for this node. Execute sync request is necessary from the leader to request and the timeout of the raft logs should be large enough to close the gap after the sync.`,
                );
                this.requestStorageSync().catch(err => {
                    logger.warn(`Error occurred while executing storage sync request`, err);
                }).finally(() => this._syncRequested = false);
                this._syncRequested = true;
            }
            if (!this._receivedEndpointNotification) {

            }
            // we send success and processed response as the problem is not with the request,
            // but we do not change our next index because we cannot process it momentary due to not synced endpoint
            var response = requestChunk.createResponse(true, logs.nextIndex, true);
            this.sendAppendEntriesResponse(response);
            return;
        } else if (this._unsyncRaftLogsLogged) {
            this._unsyncRaftLogsLogged = false;
        }

        // if we arrived in this point we know that the sync is possible.
        const entryLength = request.entries.length;
        const localNextIndex = logs.nextIndex;
        let success = true;
        for (let i = 0; i < entryLength; ++i) {
            var logIndex = request.leaderNextIndex - entryLength + i;
            var entry = request.entries[i];
            if (logIndex < localNextIndex) {
                var oldLogEntry = logs.compareAndOverride(logIndex, currentTerm, entry);
                if (oldLogEntry != null && currentTerm < oldLogEntry.term) {
                    logger.warn(`We overrode an entry coming from a higher term we currently had. 
                        (currentTerm: ${currentTerm}, old log entry term: ${oldLogEntry.term}). This can cause a potential inconsistency if other peer has not override it as well`
                    );
                }
            } else if (!logs.compareAndAdd(logIndex, currentTerm, entry)) {
                logger.warn("Log for index {} not added, though it supposed to", logIndex);
                success = false;
            }
        }
        this.updateCommitIndex(requestChunk.leaderCommit);
        var response = requestChunk.createResponse(success, logs.nextIndex, true);
        this.sendAppendEntriesResponse(response);
    }
    
    public receiveAppendEntriesResponse(response: RaftAppendEntriesResponse): void {
        if (response.destinationPeerId === undefined) {
            return;
        }
        if (response.destinationPeerId === this.getLocalPeerId()) {
            logger.warn("Follower received a raft append entries response. That should not happen as only the leader should receive this message. it is ignored", response);
            return;
        }
        if (this.leaderId !== undefined || response.destinationPeerId, this.leaderId) {
            return;
        }
        if (this.syncedProperties.currentTerm < response.term) {
            logger.warn("Follower received a raft append response, and the leader term is higher than this, so we change it");
            this.setActualLeaderId(response.destinationPeerId);
        }
    }

    public get state(): RaftState {
        return RaftState.FOLLOWER;
    }

    public run(): void {
        const config = this.config;
        const now = Date.now();
        const localPeerId = this.getLocalPeerId();
        if (this.leaderId === undefined || !this._receivedEndpointNotification) {
            // if we don't know any leader, or we have not received endpoint state notification and
            // since the sentHello is -1 by default that ensures hello is sent when state change
            // happens, which if we have a leader makes it to send the endpoint state notification
            if (config.sendingHelloTimeoutInMs < now - this._sentHello) {
                const notification = new HelloNotification(localPeerId);
                this.sendHelloNotification(notification);
                logger.debug(`Sent hello message`, notification);
                this._sentHello = now;
            }
        }
        const elapsedInMs = now - this._updated;
        if (elapsedInMs <= config.followerMaxIdleInMs + this._extraWaitingTime) {
            // we have still time before we start an election
            return;
        }
        // we don't know a leader at this point
        this.setActualLeaderId(undefined);
        if (this.remotePeers.size < 1) {
            // if we are alone, there is no point to start an election
            return;
        }

        logger.debug(`${localPeerId} is timed out to wait for append logs request (maxIdle: ${config.followerMaxIdleInMs}, elapsed: ${elapsedInMs}) Previously unsuccessful elections: ${this._timedOutElection}, extra waiting time: ${this._extraWaitingTime}`);
        this.elect(this._timedOutElection);
    }

    private updateCommitIndex(leaderCommitIndex: number) {
        const logs = this.logs;;
        if (leaderCommitIndex <= logs.commitIndex) {
            return;
        }
        const expectedCommitIndex = Math.min(logs.nextIndex - 1, leaderCommitIndex);
        const committedLogEntries = logs.commitUntil(expectedCommitIndex);
        for (const logEntry of committedLogEntries) {
            this.commitLogEntry(logEntry);
        }
    }
}