import { EventEmitter } from "ws";
import { v4 as uuid } from "uuid";
import { createLogger } from "@hamok-dev/common";

const logger = createLogger("MessageEmitter");

const ENQUEUED_REQUEST_EVENT_NAME = `ENQUEUED_REQUEST_EVENT_NAME-${uuid()}`;
const DEQUEUED_REQUEST_EVENT_NAME = `DEQUEUED_REQUEST_EVENT_NAME-${uuid()}`;

type AsyncListener = (...values: any[]) => Promise<void>;
type Listener = (...values: any[]) => void;

export type RequestListener = (requestId: string, sourceEndpointId: string) => void;

export class MessageEmitter {
    private _emitter = new EventEmitter();
    private _index = 0;
    private _invocations = new Map<number, Promise<void>>();
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

    public emit(event: string, ...args: any[]): this {
        this._emitter.emit(event, ...args);
        return this;
    }

    public addListener(event: string, listener: Listener): this {
        this._emitter.on(event, listener);
        return this;
    }

    /**
     * everything which is added here executed one by one
     */
    public addBlockingListener(event: string, listener: AsyncListener): this {
        const mappedListener: AsyncListener = async (...values: any[]) => {
            const enqueued = 0 < this._invocations.size;
            const actualBlockingPoint = this.actualBlockingPoint;
            const index = ++this._index;
            this._invocations.set(index, new Promise<void>(async resolve => {
                if (enqueued) {
                    const [requestId, sourceEndpointId] = this._getRequestIdAndSourceEndpointId(...values);
                    this._emitEnqueuedRequest(requestId, sourceEndpointId);
                    await actualBlockingPoint;
                    this._emitDequeuedRequest(requestId, sourceEndpointId);
                }
                
                listener(...values).catch((err) => {
                    logger.warn(`Error occurred while invoking listener. arguments, error:`, values, err);
                }).finally(() => {
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