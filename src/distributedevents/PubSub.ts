import { Collections, createLogger, PublishCustomDataNotification } from "@hamok-dev/common";
import { EventEmitter } from "ws";
import { PubSubBuilder } from "./PubSubBuilder";
import { PubSubComlink, PubSubComlinkConfig } from "./PubSubComlink";

const logger = createLogger("PubSub");

export type SubscriptionChangedListener = (remoteEndpointId: string, topic: string) => void;
export type PubSubListener = (
    data: Uint8Array,
    sourceEndpointId?: string, 
    event?: string, 
    topic?: string,
) => void;

const SUBSCRIPTION_ADDED_EVENT_NAME = `SubscriptionAdded-${Math.random()}-${Math.random}`;
const SUBSCRIPTION_REMOVED_EVENT_NAME = `SubscriptionRemoved-${Math.random()}-${Math.random}`;
// const CUSTOM_DATA_RECEIVED = "customDataReceived";

export type PubSubConfig = PubSubComlinkConfig & {
    /**
     * The maximum number of keys can be put into one outgoing request / response
     */
    maxKeys: number,
    /**
     * The maximum number of values can be put into one outgoing request / response
     */
    maxValues: number,
}

/**
 * ** Arch. decision **: subscription add / remove requests should go through Raft!
 * 
 * The resioning behind is to ensure the correct order of add / remove endpoints for specific topics.
 * All of the endpoint must have exactly the same replica about subscriptions, which requires to execute add / remove actions 
 * exactly the same order.
 */

export class PubSub {
    public static builder(): PubSubBuilder {
        return new PubSubBuilder();
    }
    
    public readonly config: PubSubConfig;
    private _standalone: boolean;
    private _subscriptions: Map<string, Set<string>>;
    private _comlink: PubSubComlink;
    private _emitter: EventEmitter;

    public constructor(
        comlink: PubSubComlink,
        config: PubSubConfig
    ) {
        this.config = config;
        this._standalone = true;
        this._subscriptions = new Map<string, Set<string>>();
        this._comlink = comlink;
        this._emitter = new EventEmitter();
        this._comlink
            .onAddSubscriptionRequest(async request => {
                if (!request.sourceEndpointId) {
                    return;
                }
                this._addSubscription(request.event, request.sourceEndpointId);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    // resolve the local promise hanging here
                    const response = request.createResponse(true);
                    this._comlink.sendAddSubscriptionResponse(response);
                }
            })
            .onAddSubscriptionNotification(async notification => {
                if (!notification.sourceEndpointId) {
                    return;
                }
                this._addSubscription(notification.event, notification.sourceEndpointId);
            })
            .onRemoveSubscriptionRequest(async request => {
                if (!request.sourceEndpointId) {
                    return;
                }
                this._removeSubscription(request.event, request.sourceEndpointId);
                if (request.sourceEndpointId === this._comlink.localEndpointId) {
                    // resolve the response hanging here
                    const response = request.createResponse(true);
                    this._comlink.sendRemoveSubscriptionResponse(response);
                }
            })
            .onRemoveSubscriptionNotification(async notification => {
                if (!notification.sourceEndpointId) {
                    return;
                }
                this._removeSubscription(notification.event, notification.sourceEndpointId);
            })
            .onPublishCustomDataRequest(async request => {
                const hasTopic = this._subscriptions.has(request.event);
                // logger.info(`onPublishCustomDataRequest()`, request);
                if (hasTopic) {
                    this._emitter.emit(request.event,
                        request.customData,
                        request.sourceEndpointId,
                        request.event,
                        this.config.topic
                    );
                } else {
                    logger.debug(`onPublishCustomDataRequest(): Does not have a subscription for topic ${request.event}`, request);
                }
                const response = request.createResponse(hasTopic);
                this._comlink.sendPublishCustomDataResponse(response);
            })
            .onPublishCustomDataNotification(async notification => {
                if (!this._subscriptions.has(notification.event)) {
                    logger.debug(`onPublishCustomDataNotification(): Does not have a subscription for topic ${notification.event}`, notification);
                    return;
                }
                this._emitter.emit(notification.event, 
                    notification.customData, 
                    notification.sourceEndpointId,
                    notification.event,
                    this.config.topic
                );
            })
            .onRemoteEndpointJoined(remoteEndpointId => {
                logger.warn(`onRemoteEndpointJoined() is not defined`);
            })
            .onRemoteEndpointDetached(remoteEndpointId => {
                logger.warn(`onRemoteEndpointDetached() is not defined`);
            })
            .onPubSubSyncRequested(async promise => {
                logger.warn(`onPubSubSyncRequested() is not defined`);
            })
            .onChangedLeaderId(leaderId => {
                if (!leaderId) {
                    return;
                }
                this._standalone = false;
            })
            
    }

    public onSubscriptionAdded(listener: SubscriptionChangedListener): this {
        this._emitter.on(SUBSCRIPTION_ADDED_EVENT_NAME, listener);
        return this;
    }

    public offSubscriptionAdded(listener: SubscriptionChangedListener): this {
        this._emitter.off(SUBSCRIPTION_ADDED_EVENT_NAME, listener);
        return this;
    }

    public onSubscriptionRemoved(listener: SubscriptionChangedListener): this {
        this._emitter.on(SUBSCRIPTION_REMOVED_EVENT_NAME, listener);
        return this;
    }

    public offSubscriptionRemoved(listener: SubscriptionChangedListener): this {
        this._emitter.off(SUBSCRIPTION_REMOVED_EVENT_NAME, listener);
        return this;
    }

    public async subscribe(event: string, listener: PubSubListener): Promise<void> {
        this._emitter.on(event, listener);
        if (this._standalone) {
            this._addSubscription(event, this._comlink.localEndpointId);
            return;
        }
        const remoteEndpointIds = this._subscriptions.get(event);
        if (remoteEndpointIds?.has(this._comlink.localEndpointId)) {
            // already subscribed
            return;
        }
        await this._comlink.requestAddSubscription(
            event,
            Collections.setOf(this._comlink.localEndpointId)
        );
    }

    public async unsubscribe(event: string, listener: PubSubListener): Promise<void> {
        this._emitter.off(event, listener);
        if (0 < this._emitter.listenerCount(event)) {
            return;
        }
        if (this._standalone) {
            this._removeSubscription(event, this._comlink.localEndpointId);
            return;
        }
        await this._comlink.requestRemoveSubscription(
            event,
            Collections.setOf(this._comlink.localEndpointId)
        );
    }

    public async publish(event: string, customData: Uint8Array): Promise<void> {
        const endpointIds = this._subscriptions.get(event);
        if (!endpointIds) {
            return;
        }
        if (this._standalone) {
            this._emitter.emit(event, 
                customData,
                this._comlink.localEndpointId,
                event,
                this.config.topic
            );
            return;
        }
        // const remoteEndpointIds = this._allButLocalEndpoint(endpointIds, () => {
        //     this._emitter.emit(event, 
        //         customData,
        //         this._comlink.localEndpointId,
        //         event,
        //         this.config.topic
        //     );
        // });
        // if (remoteEndpointIds.size < 1) {
        //     return;
        // }
        await this._comlink.requestPublishCustomData(
            event,
            customData,
            endpointIds
        );
    }

    public async notify(event: string, customData: Uint8Array): Promise<void> {
        const endpointIds = this._subscriptions.get(event);
        if (!endpointIds) {
            return;
        }
        if (this._standalone) {
            this._emitter.emit(event,
                customData,
                this._comlink.localEndpointId,
                event,
                this.config.topic
            );
            return;
        }
        // const remoteEndpointIds = this._allButLocalEndpoint(endpointIds, () => {
        //     this._emitter.emit(event,
        //         customData,
        //         this._comlink.localEndpointId,
        //         event,
        //         this.config.topic
        //     );
        // });
        // logger.info(`notify()`, remoteEndpointIds, endpointIds);
        const localEndpointId = this._comlink.localEndpointId;
        for (const remoteEndpointId of this._subscriptions.get(event) ?? Collections.emptySet<string>()) {
            const notification = new PublishCustomDataNotification(
                event,
                customData,
                localEndpointId,
                remoteEndpointId
            );
            this._comlink.sendPublishCustomDataNotification(notification);
        }
    }

    private _addSubscription(event: string, endpointId: string) {
        let endpointIds = this._subscriptions.get(event);
        if (!endpointIds) {
            endpointIds = new Set<string>();
            this._subscriptions.set(event, endpointIds);
        }
        if (!endpointIds.has(endpointId)) {
            endpointIds.add(endpointId);
            this._emitter.emit(SUBSCRIPTION_ADDED_EVENT_NAME, endpointIds, event);
        }
    }

    private _removeSubscription(event: string, endpointId: string) {
        const remoteEndpointIds = this._subscriptions.get(event);
        if (!remoteEndpointIds) {
            return;
        }
        if (remoteEndpointIds.delete(endpointId)) {
            this._emitter.emit(SUBSCRIPTION_REMOVED_EVENT_NAME, remoteEndpointIds, event);
        }
        if (remoteEndpointIds.size < 1) {
            this._subscriptions.delete(event);
        }
    }

    private _allButLocalEndpoint(endpointIds: ReadonlySet<string>, callbackIfHasLocalEndpoint?: () => void): Set<string> {
        const result = new Set<string>();
        const localEndpointId = this._comlink.localEndpointId;
        for (const endpointId of endpointIds) {
            if (localEndpointId !== endpointId) {
                result.add(endpointId);
                continue;
            }
            if (callbackIfHasLocalEndpoint) {
                callbackIfHasLocalEndpoint();
            }
        }
        return result;
    }
}