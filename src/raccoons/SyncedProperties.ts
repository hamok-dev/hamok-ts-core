import { 
    createLogger 
} from "@hamok-dev/common";

const logger = createLogger("SyncedProperties");

export class SyncedProperties {
    /* Persistent state on all servers: */
    /**
     * latest term server has seen (initialized to 0
     * on first boot, increases monotonically)
     */
    public get currentTerm(): number {
        return this._currentTerm;
    }

    /* Volatile state on all servers: */
    /**
     * candidateId that received vote in current
     * term (or null if none)
     */
    public get votedFor(): string | undefined {
        return this._votedFor;
    }


    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    public get lastApplied(): number {
        return this._lastApplied;
    }

    /* Volatile state on leaders (Reinitialized after election): */
    /**
     * for each server, index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    public get nextIndex(): Map<string, number> {
        return this._nextIndex;
    }

    /**
     * for each server, index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    public get matchIndex(): Map<string, number> {
        return this._matchIndex;
    }

    private _currentTerm = 0;
    private _votedFor?: string;
    private _lastApplied = -1;
    private _nextIndex = new Map<string, number>();
    private _matchIndex = new Map<string, number>();

    public set votedFor(value: string | undefined) {
        if (value !== undefined) {
            this._votedFor = value;
        } else {
            this._votedFor = undefined;
        }
    }

    public set currentTerm(value: number) {
        if (value < this._currentTerm) {
            logger.warn("Set current term smaller than the term stored before");
        }
        this._currentTerm = value;
    }
}