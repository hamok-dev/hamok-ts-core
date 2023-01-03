import { createLogger, Message } from "@hamok-dev/common";
import { CompletablePromise, CompletablePromiseState } from "../utils/CompletablePromise";

const logger = createLogger("PendingRequest");

interface Builder {
    withPendingEndpoints(endpointIds: ReadonlySet<string>): Builder;
    withTimeoutInMs(value: number): Builder
    withNeededResponse(value: number): Builder
    withThrowingTimeoutException(value: boolean): Builder
    withRequestId(value: string): Builder
    build(): PendingRequest;
}

const EMPTY_ARRAY: ReadonlyArray<Message> = [];

export class PendingRequest implements Promise<ReadonlyArray<Message>> {
    public static builder(): Builder {
        const request = new PendingRequest();
        const result: Builder = {
            withPendingEndpoints: (endpointIds: ReadonlySet<string>) => {
                endpointIds.forEach(endpointId => {
                    request._pendingEndpointIds.add(endpointId);
                });
                return result;
            },
            withTimeoutInMs: (value: number) => {
                request._timeoutInMs = value;
                return result;
            },
            withNeededResponse: (value: number) => {
                request._neededResponses = value;
                return result;
            },
            withThrowingTimeoutException: (value: boolean) => {
                request._throwTimeoutException = value;
                return result;
            },
            withRequestId: (value: string) => {
                request._id = value;
                return result;
            },
            build: () => {
                if (request._id === undefined) {
                    throw new Error(`Cannot build a pending request without id`);
                }
                const promise = new CompletablePromise<ReadonlyArray<Message>>();
                if (request._pendingEndpointIds.size < 1 && request._neededResponses < 1) {
                    promise.resolve(EMPTY_ARRAY);
                } else if (0 < request._timeoutInMs) {
                    request._timer = setTimeout(() => {
                        if (request._timer === undefined) {
                            return;
                        }
                        request._timer = undefined;
                        if (promise.state !== CompletablePromiseState.PENDING) {
                            return;
                        }
                        request._timedOut = true;
                        if (request._throwTimeoutException) {
                            promise.reject(new Error(`Timeout occurred at pending promise ` + request));
                        } else {
                            logger.warn(`Pending request is timed out and resolved with missing responses. ${this.toString()}`);
                            const response = Array.from(request._responses.values());
                            promise.resolve(response);
                        }
                    }, request._timeoutInMs);
                }
                return request;
            },
        }
        return result;
    }

    private _id?: string;
    private _timedOut = false;
    private _timeoutInMs = 0;
    private _receivedResponses = 0;
    private _neededResponses = -1;
    private _throwTimeoutException = true;
    private _pendingEndpointIds = new Set<string>();
    private _responses = new Map<string, Message>();
    private _timer?: ReturnType<typeof setTimeout>;
    private _promise?: CompletablePromise<Message[]>
    private constructor() {

    }
    
    public get id(): string {
        return this._id!;
    }

    public then<TResult1 = Message[], TResult2 = never>(onfulfilled?: ((value: Message[]) => TResult1 | PromiseLike<TResult1>) | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null): Promise<TResult1 | TResult2> {
        return this._promise!.then(onfulfilled, onrejected);
    }
    
    public catch<TResult = never>(onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null): Promise<Message[] | TResult> {
        return this._promise!.catch(onrejected);
    }
    
    public finally(onfinally?: (() => void) | null): Promise<Message[]> {
        return this._promise!.finally(onfinally);
    }

    public accept(message: Message): void {
        if (message.sourceId === undefined || message.requestId === undefined) {
            logger.warn(`No source or request id is assigned for message: ${message}`);
            return;
        }
        let completed = false;
        try {
            const pendingBefore = this._pendingEndpointIds.size;
            if (!this._pendingEndpointIds.delete(message.sourceId) && this._neededResponses < 1) {
                logger.debug(`Source endpoint ${message.sourceId} is not found 
                    in pending ids of request ${message.requestId}`
                );
                completed = pendingBefore == 0;
                return;
            }
            const pendingAfter = this._pendingEndpointIds.size;
            const prevResponse = this._responses.get(message.sourceId);
            if (prevResponse) {
                logger.warn(`Remote endpoint ${message.sourceId} overrided its previous response for request ${this.id}. removed response`, message);
            }
            this._responses.set(message.sourceId, message);
            ++this._receivedResponses;
            logger.trace(`Id: ${this.id}, pending before:`)
            completed = (pendingBefore == 1 && pendingAfter == 0) || (0 < this._neededResponses && this._receivedResponses <= this._neededResponses);
        } finally {
            if (completed) {
                this._resolve();
            }
        }
    }

    public removeEndpointId(endpointId: string) {
        const removed = this._pendingEndpointIds.delete(endpointId);
        if (!removed || this._promise!.state !== CompletablePromiseState.PENDING) {
            return;
        }
        if (this._pendingEndpointIds.size < 1 && this._neededResponses < 0) {
            this._resolve();
        }
    }

    public addPendingEndpointId(endpointId: string): void {
        this._pendingEndpointIds.add(endpointId);
    }

    public isDone(): boolean {
        return this._promise!.state !== CompletablePromiseState.PENDING;
    }

    public isTimedOut(): boolean {
        return this._timedOut;
    }

    public getRemainingEndpointIds(): ReadonlySet<string> {
        return this._pendingEndpointIds;
    }

    private _resolve(): void {
        if (this._timer) {
            clearTimeout(this._timer);
            this._timer = undefined;
        }
        if (this._promise!.state !== CompletablePromiseState.PENDING) {
            logger.warn(`Attempted to resolve a not pending request (${this})`);
            return;
        }
        const response = Array.from(this._responses.values());
        logger.trace(`Pending request is resolved by responses ${response}`);
        this._promise!.resolve(response);
    }

    public get [Symbol.toStringTag](): string {
        const acceptedEndpointIds = Array.from(this._responses.keys()).join(", ");
        return `Pending request id: ${this._id}, 
                received responses: ${this._receivedResponses}, 
                remaining endpoints: ${this._pendingEndpointIds}, 
                accepted endpoints: ${acceptedEndpointIds} 
                timeoutInMs: ${this._timeoutInMs}`;
    }
}