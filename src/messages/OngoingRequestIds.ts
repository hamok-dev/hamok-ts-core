import { createLogger, OngoingRequestsNotification } from "@hamok-dev/common"

const logger = createLogger("OngoingRequestIds");

type Sender = (notification: OngoingRequestsNotification) => void;

export class OngoingRequestIds {
    private _sender?: Sender;
    private _ongoingRequestIds = new Map<string, Set<string>>();
    private _timer?: ReturnType<typeof setTimeout>;
    private _timeoutInMs: number;

    public constructor(timeoutInMs: number) {
        this._timeoutInMs = timeoutInMs;
    }

    public set sender(value: Sender) {
        if (this._sender) {
            logger.warn(`Ongoing request notification sender was already set, now its overrided`);
        }
        this._sender = value;
    }

    public addOngoingRequestId(requestId: string, remoteEndpointId: string) {
        let ongoingRequestIds = this._ongoingRequestIds.get(remoteEndpointId);
        if (!ongoingRequestIds) {
            ongoingRequestIds = new Set<string>();
            this._ongoingRequestIds.set(remoteEndpointId, ongoingRequestIds);
        }
        ongoingRequestIds.add(requestId);
        if (!this._timer) {
            this._startTimer();
        }
    }

    public removeOngoingRequestId(requestId: string): boolean {
        let remoteEndpointId: string | undefined;
        for (const [iteratedRemoteEndpointId, ongoingRequestIds] of this._ongoingRequestIds.entries()) {
            if (ongoingRequestIds.has(requestId)) {
                remoteEndpointId = iteratedRemoteEndpointId;
                break;
            }
        }
        if (!remoteEndpointId) {
            return false;
        }
        const ongoingRequestIds = this._ongoingRequestIds.get(requestId);
        if (!ongoingRequestIds) {
            return false;
        }
        const result = ongoingRequestIds.delete(requestId);
        if (ongoingRequestIds.size < 1) {
            this._ongoingRequestIds.delete(remoteEndpointId);
            if (this._ongoingRequestIds.size < 1) {
                this._stopTimer();
            }
        }
        return result;
    }

    private _startTimer(): void {
        if (this._timer) {
            logger.warn(`Attempted to start a timer twice`);
            return;
        }
        const process = () => {
            if (this._ongoingRequestIds.size < 1) {
                this._timer = undefined;
                return;
            }
            const sender = this._sender;
            if (!sender) {
                logger.warn(`Cannot send notification, becasue the sender is undefined`);
                return;
            }
            for (const [remoteEndpointId, ongoingRequestIds] of this._ongoingRequestIds) {
                const notification = new OngoingRequestsNotification(
                    ongoingRequestIds,
                    remoteEndpointId
                );
                sender(notification);
            }
            this._timer = setTimeout(process, this._timeoutInMs);
        };
        this._timer = setTimeout(process, this._timeoutInMs);
    }

    private _stopTimer(): void {
        if (!this._timer) {
            return;
        }
        clearTimeout(this._timer);
        this._timer = undefined;
    }
}