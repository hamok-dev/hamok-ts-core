import { EventEmitter } from "ws";
import { v4 as uuid } from "uuid";
import { createLogger } from "@hamok-dev/common";

const logger = createLogger("MessageEmitter");

const ENQUEUED_REQUEST_EVENT_NAME = `ENQUEUED_REQUEST_EVENT_NAME-${uuid()}`;
const DEQUEUED_REQUEST_EVENT_NAME = `DEQUEUED_REQUEST_EVENT_NAME-${uuid()}`;

// eslint-disable @typescript-eslint/no-explicit-any
type AsyncListener = (...values: any[]) => Promise<void>;

// eslint-disable @typescript-eslint/no-explicit-any
type Listener = (...values: any[]) => void;

export type RequestListener = (requestId: string, sourceEndpointId: string) => void;

// export interface MessageEmitterEvents {
//     'enqueued-request': {
//         requestId: string,
//         sourceEndpointId: string,
//     },
//     'dequeued-request': {
//         requestId: string,
//         sourceEndpointId: string,
//     }
// }

export class MessageEmitter {
    private _emitter = new EventEmitter();
    private _index = 0;
    private _invocations = new Map<number, Promise<void>>();
    
    // eslint-disable @typescript-eslint/no-explicit-any
    private _blockingListeners = new Map<any, AsyncListener>();

    public get queueSize(): number {
        return this._invocations.size;
    }

    public get actualBlockingPoint(): Promise<void> {
        return this._invocations.get(this._index) ?? Promise.resolve();
    }

    public onEnqueuedRequest(listener: RequestListener): this {
        this._emitter.on(ENQUEUED_REQUEST_EVENT_NAME, listener);
        return this;
    }

    public offEnqueuedRequest(listener: RequestListener): this {
        this._emitter.off(ENQUEUED_REQUEST_EVENT_NAME, listener);
        return this;
    }

    public onDequeuedRequest(listener: RequestListener): this {
        this._emitter.on(DEQUEUED_REQUEST_EVENT_NAME, listener);
        return this;
    }

    public offDequeuedRequest(listener: RequestListener): this {
        this._emitter.off(DEQUEUED_REQUEST_EVENT_NAME, listener);
        return this;
    }

    // eslint-disable @typescript-eslint/no-explicit-any
    public emit(event: string, ...args: any[]): this {
        // logger.info("emitted ", event, ...args);
        this._emitter.emit(event, ...args);
        return this;
    }

    public addListener(event: string, listener: Listener): this {
        this._emitter.on(event, listener);
        return this;
    }

    public async clear(): Promise<void> {
        this._emitter.eventNames().forEach(eventName => this._emitter.removeAllListeners(eventName));
        await this.actualBlockingPoint;
        this._blockingListeners.clear();
        this._invocations.clear();
        this._index = 0;
    }

    /**
     * everything which is added here executed one by one
     */
    public addBlockingListener(event: string, listener: AsyncListener): this {
        // eslint-disable @typescript-eslint/no-explicit-any
        const mappedListener: AsyncListener = async (...values: any[]) => {
            const enqueued = 0 < this._invocations.size;
            const actualBlockingPoint = this.actualBlockingPoint;
            const index = ++this._index;
            this._invocations.set(index, new Promise<void>(resolve => {
                // logger.info("Invocation for ", values);
                let promise: Promise<void> | undefined;
                if (enqueued) {
                    const [requestId, sourceEndpointId] = this._getRequestIdAndSourceEndpointId(...values);
                    this._emitEnqueuedRequest(requestId, sourceEndpointId);
                    logger.trace(`Invocation is enqueued for event ${event}, index: ${index}, queue size: ${this._invocations.size}`);
                    promise = actualBlockingPoint.then(() => new Promise(resolveBlocking => {
                        this._emitDequeuedRequest(requestId, sourceEndpointId);
                        logger.trace(`Dequeued for event ${event}, index: ${index}, queue size: ${this._invocations.size}`);
                        resolveBlocking();
                    }));
                } else {
                    promise = Promise.resolve();
                }
                promise
                    .then(() => listener(...values))
                    .catch(err => {
                        logger.warn(`Error occurred while invoking listener. arguments, error:`, values, err);
                    })
                    .finally(() => {
                        this._invocations.delete(index);
                        resolve();
                    });
            }));
        };
        this._blockingListeners.set(listener, mappedListener);
        this._emitter.addListener(event, mappedListener);
        return this;
    }

    public removeListener(event: string, listener: AsyncListener | Listener): this {
        const actualListener = this._blockingListeners.get(listener) ?? listener;
        this._emitter.removeListener(event, actualListener);
        return this;
    }

    public removeAllListeners(event?: string): this {
        this._emitter.removeAllListeners(event);
        return this;
    }

    private _emitEnqueuedRequest(requestId?: string, sourceEndpointId?: string): void {
        if (!requestId || !sourceEndpointId) return;
        this._emitter.emit(ENQUEUED_REQUEST_EVENT_NAME, requestId, sourceEndpointId);
    }

    private _emitDequeuedRequest(requestId?: string, sourceEndpointId?: string): void {
        if (!requestId || !sourceEndpointId) return;
        this._emitter.emit(DEQUEUED_REQUEST_EVENT_NAME, requestId, sourceEndpointId);
    }
    
    private _getRequestIdAndSourceEndpointId(...values: any[]): [string | undefined, string | undefined] {
        let requestId: string | undefined;
        let sourceEndpointId: string | undefined;
        for (const value of values) {
            if (!requestId) {
                requestId = value?.requestId;
            }
            if (!sourceEndpointId) {
                sourceEndpointId = value?.sourceId ?? value?.sourceEndpointId;
            }
        }
        return [requestId, sourceEndpointId];
    }
}