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

const logger = createLogger("CandidateState");

export class CandidateState extends RaccoonState {
    private _started = -1;
    private _notifiedRemotePeers = new Set<string>();
    private _receivedVotes = new Set<string>();
    private _electionTerm: number;
    private _wonTheElection = false;
    private _prevTimedOutElection: number;
    private _respondedRemotePeerIds = new Set<string>();

    public constructor(
        base: RaccoonBase,
        prevTimedOutElection: number
    ) {
        super(base);
        this._prevTimedOutElection = prevTimedOutElection;
        this._electionTerm = this.syncedProperties.currentTerm + 1;
        this.setActualLeaderId(undefined);
    }

    public get state(): RaftState {
        return RaftState.CANDIDATE;
    }

    public submit(message: Message): boolean {
        logger.debug(`got a submit for message ${message}`);
        return false;
    }

    public receiveHelloNotification(notification: HelloNotification): void {
        logger.debug("Cannot process a received notification in candidate state {}", notification);
    }
    
    public receiveVoteRequest(request: RaftVoteRequest): void {
        logger.trace(`received Vote request. ${request}`);
        const response = request.createResponse(false);
        this.sendVoteResponse(response);
    }

    public receiveVoteResponse(response: RaftVoteResponse): void {
        const remotePeers = this.remotePeers;
        if (remotePeers.get(response.sourcePeerId) !== undefined) {
            // let's keep up to date the last touches
            remotePeers.touch(response.sourcePeerId);
        }
        if (this._electionTerm < response.term) {
            // election should dismiss, case should be closed
            logger.warn(`Candidate received response from a higher term (${response.term}) than the current election term (${this._electionTerm}).`, );
            this.follow();
            return;
        }
        if (response.term < this._electionTerm) {
            logger.warn(`A vote response from a term smaller than the current is received:`, response);
            return;
        }
        this._respondedRemotePeerIds.add(response.sourcePeerId);
        if (!response.voteGranted) {
            return;
        }
        this._receivedVotes.add(response.sourcePeerId);

        const numberOfPeerIds = this.remotePeers.size + 1; // +1, because of a local raccoon!
        logger.debug(`Received vote for leadership: ${this._receivedVotes.size}, 
            number of peers: ${numberOfPeerIds}. activeRemoteEndpointIds: ${Array.from(remotePeers.getRemotePeerIds()).join(", ")}`
        );
        if (numberOfPeerIds < this._receivedVotes.size * 2) {
            this._wonTheElection = true;
        }
        else if (this._notifiedRemotePeers.size < numberOfPeerIds) {
            // might happens that a new remote endpoint has been discovered since we started the process.
            // in this case we resent the vote requests
            for (const remotePeerId of remotePeers.getRemotePeerIds()) {
                if (!this._notifiedRemotePeers.has(remotePeerId)) {
                    const request = new RaftVoteRequest(
                        this._electionTerm,
                        this.logs.nextIndex - 1,
                        this.syncedProperties.currentTerm,
                        remotePeerId,
                        this.getLocalPeerId(),
                    );
                    this.sendVoteRequest(request);
                    logger.info("Send vote request to {} after the candidacy has been started", request);
                }
            }
        }
    }

    public receiveEndpointStatesNotification(notification: EndpointStatesNotification): void {
        logger.debug("Cannot process a received notification in candidate state", notification);
    }

    public receiveAppendEntriesRequest(request: RaftAppendEntriesRequestChunk): void {
        // a leader has been elected, let's go back to the follower state
        this.follow();
    }

    public receiveAppendEntriesResponse(response: RaftAppendEntriesResponse): void {
        if (response.destinationPeerId !== undefined) {
            logger.warn(`{} cannot process a received response in candidate state ${response}`);
        }
    }


    public start(): void {
        // voting for myself!
        this._receivedVotes.add(this.getLocalPeerId());

        // to execute async
        (async () => {
            const config = this.config;
            const props = this.syncedProperties;
            const logs = this.logs;
            const remotePeers = this.remotePeers;
            for (const peerId of remotePeers.getRemotePeerIds()) {
                const request = new RaftVoteRequest(
                        this._electionTerm,
                        logs.nextIndex - 1,
                        props.currentTerm,
                        peerId,
                        this.getLocalPeerId(),
                );
                this.sendVoteRequest(request);
                logger.info(`Send vote request to`, request);
                this._notifiedRemotePeers.add(peerId);
            }
            this._started = Date.now();
        })();
    }

    public run(): void {
        if (this._started < 0) {
            return;
        }
        const config = this.config;
        const elapsedTimeInMs = Date.now() - this._started;
        if (this._wonTheElection) {
            logger.debug("Won the election");
            this.lead();
            return;
        }
        if (config.electionTimeoutInMs < elapsedTimeInMs) {
            // election timeout
            logger.warn(`Timeout occurred during the election process 
                (electionTimeoutInMs: ${config.electionTimeoutInMs}, 
                elapsedTimeInMs: ${elapsedTimeInMs}, 
                respondedRemotePeerIds: ${Array.from(this._respondedRemotePeerIds).join(", ")}). 
                This can be a result because of split vote. 
                previously timed out elections: ${this._prevTimedOutElection}. elapsedTimeInMs: ${elapsedTimeInMs}`, 
            );
            this.follow(this._prevTimedOutElection + 1);
            return;
        }
    }

}