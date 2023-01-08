import { Timestamp } from "@bufbuild/protobuf";
import { createLogger, PublishCustomDataNotification } from "@hamok-dev/common";
import { EventEmitter } from "ws";
import { PubSubComlink, PubSubComlinkConfig } from "./PubSubComlink";

const logger = createLogger("PubSub");

export type SubscriptionChangedListener = (remoteEndpointId: string, topic: string) => void;
export type CustomDataListener = (data: Uint8Array, topic: string) => void;

const SUBSCRIPTION_ADDED_EVENT_NAME = "SubscriptionAdded";
const SUBSCRIPTION_REMOVED_EVENT_NAME = "SubscriptionRemoved";
const CUSTOM_DATA_RECEIVED = "customDataReceived";

export type PubSubConfig = PubSubComlinkConfig & {
    
}

/**
 * ** Arch. decision **: subscription add / remove requests should go through Raft!
 * 
 * The resioning behind is to ensure the correct order of add / remove endpoints for specific topics.
 * All of the endpoint must have exactly the same replica about subscriptions, which requires to execute add / remove actions 
 * exactly the same order.
 */

export class PubSub {
    
    public config: PubSubConfig;
    private _subscriptions: Map<string, Set<string>>;
    private _comlink: PubSubComlink;
    private _emitter: EventEmitter;

    public constructor(
        comlink: PubSubComlink,
        config: PubSubConfig
    ) {
        this.config = config;
        this._subscriptions = new Map<string, Set<string>>();
        this._comlink = comlink;
        this._emitter = new EventEmitter();
        this._comlink
            .onAddSubscriptionRequest(async request => {
                if (!request.sourceEndpointId) {
                    return;
                }
                let remoteEndpointIds = this._subscriptions.get(request.event);
                if (!remoteEndpointIds) {
                    remoteEndpointIds = new Set<string>();
                    this._subscriptions.set(request.event, remoteEndpointIds);
                }
                if (!remoteEndpointIds.has(request.sourceEndpointId)) {
                    remoteEndpointIds.add(request.sourceEndpointId);
                    this._emitter.emit(SUBSCRIPTION_ADDED_EVENT_NAME, remoteEndpointIds, request.event);
                }
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
                let remoteEndpointIds = this._subscriptions.get(notification.event);
                if (!remoteEndpointIds) {
                    remoteEndpointIds = new Set<string>();
                    this._subscriptions.set(notification.event, remoteEndpointIds);
                }
                if (!remoteEndpointIds.has(notification.sourceEndpointId)) {
                    remoteEndpointIds.add(notification.sourceEndpointId);
                    this._emitter.emit(SUBSCRIPTION_ADDED_EVENT_NAME, remoteEndpointIds, notification.event);
                }
            })
            .onRemoveSubscriptionRequest(async request => {
                if (!request.sourceEndpointId) {
                    return;
                }
                const remoteEndpointIds = this._subscriptions.get(request.event);
                if (!remoteEndpointIds) {
                    this._comlink.sendRemoveSubscriptionResponse(
                        request.createResponse(true)
                    );
                    return;
                }
                if (remoteEndpointIds.delete(request.sourceEndpointId)) {
                    this._emitter.emit(SUBSCRIPTION_REMOVED_EVENT_NAME, remoteEndpointIds, request.event);
                }
                if (remoteEndpointIds.size < 1) {
                    this._subscriptions.delete(request.event);
                }
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
                const remoteEndpointIds = this._subscriptions.get(notification.event);
                if (!remoteEndpointIds) {
                    return;
                }
                if (remoteEndpointIds.delete(notification.sourceEndpointId)) {
                    this._emitter.emit(SUBSCRIPTION_REMOVED_EVENT_NAME, remoteEndpointIds, notification.event);
                }
                if (remoteEndpointIds.size < 1) {
                    this._subscriptions.delete(notification.event);
                }
            })
            .onPublishCustomDataRequest(async request => {
                const hasTopic = this._subscriptions.has(request.event);
                if (hasTopic) {
                    this._emitter.emit(CUSTOM_DATA_RECEIVED, request.customData, request.event);
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
                this._emitter.emit(CUSTOM_DATA_RECEIVED, notification.customData, notification.event);
            })
            .onRemoteEndpointJoined(remoteEndpointId => {

            })
            .onRemoteEndpointDetached(remoteEndpointId => {

            })
            .onPubSubSyncRequested(async promise => {
                
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

    public async subscribe(topic: string, listener: CustomDataListener): Promise<void> {
        this._emitter.on(topic, listener);
        if (this._subscriptions.has(topic)) {
            // already subscribed
            return;
        }
        await this._comlink.requestAddSubscription(topic);
    }

    public async unsubscribe(topic: string, listener: CustomDataListener): Promise<void> {
        this._emitter.off(topic, listener);
        if (0 < this._emitter.listenerCount(topic)) {
            return;
        }
        await this._comlink.requestRemoveSubscription(topic);
    }

    public async publish(topic: string, customData: Uint8Array): Promise<void> {
        const endpointIds = this._subscriptions.get(topic);
        if (!endpointIds) {
            return;
        }
        const remoteEndpointIds = this._allButLocalEndpoint(endpointIds, () => {
            this._emitter.emit(topic, customData);
        });
        if (remoteEndpointIds.size < 1) {
            return;
        }
        await this._comlink.requestPublishCustomData(
            topic,
            customData,
            remoteEndpointIds
        );
    }

    public async notify(topic: string, customData: Uint8Array): Promise<void> {
        const endpointIds = this._subscriptions.get(topic);
        if (!endpointIds) {
            return;
        }
        const remoteEndpointIds = this._allButLocalEndpoint(endpointIds, () => {
            this._emitter.emit(topic, customData);
        });
        const localEndpointId = this._comlink.localEndpointId;
        for (const remoteEndpointId of remoteEndpointIds) {
            const notification = new PublishCustomDataNotification(
                topic,
                customData,
                localEndpointId,
                remoteEndpointId
            );
            this._comlink.sendPublishCustomDataNotification(notification);
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