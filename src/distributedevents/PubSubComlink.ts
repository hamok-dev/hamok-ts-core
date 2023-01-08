import { 
    Message,
    MessageType, 
    MessageProcessor, 
    createLogger,
    MessageDefaultProcessor,
    AddSubscriptionRequest,
    AddSubscriptionResponse,
    AddSubscriptionNotification,
    RemoveSubscriptionNotification,
    RemoveSubscriptionRequest,
    RemoveSubscriptionResponse,
    PublishCustomDataRequest,
    PublishCustomDataResponse,
    PublishCustomDataNotification,
    GetSubscriptionsRequest,
    GetSubscriptionsResponse,
    PubSubCodec,
} from "@hamok-dev/common"
import { PendingRequest } from "../messages/PendingRequest";
import { v4 as uuid } from "uuid";
import { HamokGrid, PubSubSyncResult } from "../HamokGrid";
import { PendingResponse } from "../messages/PendingResponse";
import { ResponseChunker } from "../messages/ResponseChunker";
import { RemoteEndpointStateChangedListener } from "../raccoons/RemotePeers";
import { CompletablePromise } from "../utils/CompletablePromise";
import { MessageEmitter } from "../messages/MessageEmitter";

const logger = createLogger("StorageComlink");

const STORAGE_SYNC_REQUESTED_EVENT_NAME = "StorageSyncRequested";
const CHANGED_LEADER_ID_EVENT_NAME = "ChangedLeaderId";
const ADD_SUBSCRIPTION_REQUEST = MessageType.ADD_SUBSCRIPTION_REQUEST.toString();
const ADD_SUBSCRIPTION_NOTIFICATION = MessageType.ADD_SUBSCRIPTION_NOTIFICATION.toString();
const REMOVE_SUBSCRIPTION_REQUEST = MessageType.REMOVE_SUBSCRIPTION_REQUEST.toString();
const REMOVE_SUBSCRIPTION_NOTIFICATION = MessageType.REMOVE_SUBSCRIPTION_NOTIFICATION.toString();
const PUBLISH_CUSTOM_DATA_REQUEST = MessageType.PUBLISH_CUSTOM_DATA_REQUEST.toString();
const PUBLISH_CUSTOM_DATA_NOTIFICATION = MessageType.PUBLISH_CUSTOM_DATA_NOTIFICATION.toString();
const GET_SUBSCRIPTIONS_REQUEST = MessageType.GET_SUBSCRIPTIONS_REQUEST.toString();

export type ChangedLeaderIdListener = (newLeaderId?: string) => void;
export type PubSubSyncRequestedListener = (request: CompletablePromise<PubSubSyncResult>) => Promise<void>;
export type AddSubscriptionRequestListener = (request: AddSubscriptionRequest) => Promise<void>;
export type AddSubscriptionNotificationListener = (notification: AddSubscriptionNotification) => Promise<void>;

export type RemoveSubscriptionRequestListener = (request: RemoveSubscriptionRequest) => Promise<void>;
export type RemoveSubscriptionNotificationListener = (notification: RemoveSubscriptionNotification) => Promise<void>;

export type PublishCustomDataRequestListener = (request: PublishCustomDataRequest) => Promise<void>;
export type PublishCustomDataNotificationListener = (notification: PublishCustomDataNotification) => Promise<void>;

export type GetSubscriptionsRequestListener = (request: GetSubscriptionsRequest) => Promise<void>;

export type PubSubComlinkConfig = {
    /**
     * The topic of the PubSub
     */
    topic: string,
    /**
     * Flag indicate if the comlink throws exception if a request is timed out or not
     */
    throwExceptionOnTimeout: boolean,

    /**
     * The timeout value in millis for a request allow a remote endpoint to process it.
     * if the value is less than 1 the request timeout is infinity
     * 
     * NOTE: remote endpoint can postpone the timeout by sending a request ongoing notification
     */
    requestTimeoutInMs: number,

    /**
     * The number of response necessay to resolve a request.
     */
    neededResponse: number,
}

export interface PubSubGridLink {
    readonly topic: string;
    receive(message: Message): void;
    requestPubSubSync(): Promise<PubSubSyncResult>;
}


export type StorageEncoder<T> = (input: T) => Uint8Array;
export type StorageDecoder<T> = (input: Uint8Array) => T;


export abstract class PubSubComlink implements PubSubGridLink {
    private _config: PubSubComlinkConfig;
    private _grid: HamokGrid;
    private _receiver: MessageProcessor<void>;
    private _emitter = new MessageEmitter();
    private _pendingRequests = new Map<string, PendingRequest>();
    private _pendingResponses = new Map<string, PendingResponse>();
    private _responseChunker: ResponseChunker;
    private _codec: PubSubCodec;
    public constructor(
        config: PubSubComlinkConfig,
        grid: HamokGrid,
        responseChunker: ResponseChunker,
        codec: PubSubCodec,
    ) {
        this._codec = codec;
        this._config = config;
        this._grid = grid;
        this._responseChunker = responseChunker;
        this._receiver = this._createReceiver();

        this._grid.onLeaderChanged(event => {
            const { actualLeaderId } = event;
            this._emitter.emit(CHANGED_LEADER_ID_EVENT_NAME, actualLeaderId);
        }).onRemoteEndpointDetached(remoteEndpointId => {
            for (const pendingRequest of this._pendingRequests.values()) {
                pendingRequest.removeEndpointId(remoteEndpointId);
            }
            // for (const pendingResponse of this._pendingResponses.values()) {
            //     if (pendingResponse.sourceEndpointId === remoteEndpointId) {
            //         pendingResponse.cancel();
            //     }
            // }
        })
        
    }

    public get topic(): string {
        return this._config.topic;
    }

    public get localEndpointId(): string {
        return this._grid.localEndpointId;
    }

    public get remoteEndpointIds(): ReadonlySet<string> {
        return this._grid.remoteEndpointIds;
    }

    public receive(message: Message) {
        this._receiver.process(message);
    }

    public async requestPubSubSync(): Promise<PubSubSyncResult> {
        const promise = new CompletablePromise<PubSubSyncResult>();
        this._emitter.emit(STORAGE_SYNC_REQUESTED_EVENT_NAME, promise);
        return promise;
    }

    public onPubSubSyncRequested(listener: PubSubSyncRequestedListener): PubSubComlink {
        this._emitter.addBlockingListener(STORAGE_SYNC_REQUESTED_EVENT_NAME, listener);
        return this;
    }

    public offStorageSyncRequested(listener: PubSubSyncRequestedListener): PubSubComlink {
        this._emitter.removeListener(STORAGE_SYNC_REQUESTED_EVENT_NAME, listener);
        return this;
    }

    public onChangedLeaderId(listener: ChangedLeaderIdListener): PubSubComlink {
        this._emitter.addListener(CHANGED_LEADER_ID_EVENT_NAME, listener);
        return this;
    }

    public offChangedLeaderId(listener: ChangedLeaderIdListener): PubSubComlink {
        this._emitter.removeListener(CHANGED_LEADER_ID_EVENT_NAME, listener);
        return this;
    }

    public onRemoteEndpointJoined(listener: RemoteEndpointStateChangedListener): PubSubComlink {
        this._grid.onRemoteEndpointJoined(listener);
        return this;
    }

    public offRemoteEndpointJoined(listener: RemoteEndpointStateChangedListener): PubSubComlink {
        this._grid.offRemoteEndpointJoined(listener);
        return this;
    }
    
    public onRemoteEndpointDetached(listener: RemoteEndpointStateChangedListener): PubSubComlink {
        this._grid.onRemoteEndpointDetached(listener);
        return this;
    }

    public offRemoteEndpointDetached(listener: RemoteEndpointStateChangedListener): PubSubComlink {
        this._grid.offRemoteEndpointDetached(listener);
        return this;
    }
    
    public onAddSubscriptionRequest(listener: AddSubscriptionRequestListener): PubSubComlink {
        this._emitter.addBlockingListener(ADD_SUBSCRIPTION_REQUEST, listener);
        return this;
    }

    public offAddSubscriptionRequest(listener: AddSubscriptionRequestListener): PubSubComlink {
        this._emitter.removeListener(ADD_SUBSCRIPTION_REQUEST, listener);
        return this;
    }

    public onAddSubscriptionNotification(listener: AddSubscriptionNotificationListener): PubSubComlink {
        this._emitter.addBlockingListener(ADD_SUBSCRIPTION_NOTIFICATION, listener);
        return this;
    }

    public offAddSubscriptionNotification(listener: AddSubscriptionNotificationListener): PubSubComlink {
        this._emitter.removeListener(ADD_SUBSCRIPTION_NOTIFICATION, listener);
        return this;
    }


    public onRemoveSubscriptionRequest(listener: RemoveSubscriptionRequestListener): PubSubComlink {
        this._emitter.addBlockingListener(REMOVE_SUBSCRIPTION_REQUEST, listener);
        return this;
    }

    public offRemoveSubscriptionRequest(listener: RemoveSubscriptionRequestListener): PubSubComlink {
        this._emitter.removeListener(REMOVE_SUBSCRIPTION_REQUEST, listener);
        return this;
    }

    public onRemoveSubscriptionNotification(listener: RemoveSubscriptionNotificationListener): PubSubComlink {
        this._emitter.addBlockingListener(REMOVE_SUBSCRIPTION_NOTIFICATION, listener);
        return this;
    }

    public offRemoveSubscriptionNotification(listener: RemoveSubscriptionNotificationListener): PubSubComlink {
        this._emitter.removeListener(REMOVE_SUBSCRIPTION_NOTIFICATION, listener);
        return this;
    }


    public onPublishCustomDataRequest(listener: PublishCustomDataRequestListener): PubSubComlink {
        this._emitter.addBlockingListener(PUBLISH_CUSTOM_DATA_REQUEST, listener);
        return this;
    }

    public offPublishCustomDataRequest(listener: PublishCustomDataRequestListener): PubSubComlink {
        this._emitter.removeListener(PUBLISH_CUSTOM_DATA_REQUEST, listener);
        return this;
    }

    public onPublishCustomDataNotification(listener: PublishCustomDataNotificationListener): PubSubComlink {
        this._emitter.addListener(PUBLISH_CUSTOM_DATA_NOTIFICATION, listener);
        return this;
    }

    public offPublishCustomDataNotification(listener: PublishCustomDataNotificationListener): PubSubComlink {
        this._emitter.removeListener(PUBLISH_CUSTOM_DATA_NOTIFICATION, listener);
        return this;
    }

    public onGetSubscriptionsRequest(listener: GetSubscriptionsRequestListener): PubSubComlink {
        this._emitter.addListener(GET_SUBSCRIPTIONS_REQUEST, listener);
        return this;
    }

    public offGetSubscriptionsRequest(listener: GetSubscriptionsRequestListener): PubSubComlink {
        this._emitter.removeListener(GET_SUBSCRIPTIONS_REQUEST, listener);
        return this;
    }

    public async requestAddSubscription(
        event: string,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<void> {
        const requestId = uuid();
        const message = this._codec.encode(
            new AddSubscriptionRequest(
                requestId,
                event
            )
        );
        await this._request(message, targetEndpointIds);
    }

    public sendAddSubscriptionResponse(response: AddSubscriptionResponse): void {
        const message = this._codec.encode(response);
        this._dispatchResponse(message);
    }

    public sendAddSubscriptionNotification(notification: AddSubscriptionNotification): void {
        const message = this._codec.encode(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }


    public async requestRemoveSubscription(
        event: string,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<void> {
        const requestId = uuid();
        const message = this._codec.encode(
            new RemoveSubscriptionRequest(
                requestId,
                event
            )
        );
        await this._request(message, targetEndpointIds);
    }

    public sendRemoveSubscriptionResponse(response: RemoveSubscriptionResponse): void {
        const message = this._codec.encode(response);
        this._dispatchResponse(message);
    }

    public sendRemoveSubscriptionNotification(notification: RemoveSubscriptionNotification): void {
        const message = this._codec.encode(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }


    public async requestPublishCustomData(
        event: string,
        customData: Uint8Array,
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<void> {
        const requestId = uuid();
        const message = this._codec.encode(
            new PublishCustomDataRequest(
                requestId,
                event,
                customData,
            )
        );
        await this._request(message, targetEndpointIds);
    }

    public sendPublishCustomDataResponse(response: PublishCustomDataResponse): void {
        const message = this._codec.encode(response);
        this._dispatchResponse(message);
    }

    public sendPublishCustomDataNotification(notification: PublishCustomDataNotification): void {
        const message = this._codec.encode(notification);
        this._dispatchNotification(
            message, 
            notification.destinationEndpointId
        );
    }

    public async requestGetSubscriptions(
        targetEndpointIds?: ReadonlySet<string>
    ): Promise<void> {
        const requestId = uuid();
        const message = this._codec.encode(
            new GetSubscriptionsRequest(
                requestId,
            )
        );
        await this._request(message, targetEndpointIds);
    }

    public sendGetSubscriptionsResponse(response: GetSubscriptionsResponse): void {
        const message = this._codec.encode(response);
        this._dispatchResponse(message);
    }

    private async _request(message: Message, targetEndpointIds?: ReadonlySet<string>, attempt?: number): Promise<Message[]> {
        if (!message.requestId) {
            logger.warn(`Cannot send request message without a requestId`, message);
            return Promise.resolve([]);
        }
        const destinationEndpointIds = targetEndpointIds ?? this.defaultResolvingEndpointIds();
        const pendingRequest = PendingRequest.builder()
            .withRequestId(message.requestId)
            .withPendingEndpoints(destinationEndpointIds)
            .withTimeoutInMs(this._config.requestTimeoutInMs)
            .withThrowingTimeoutException(this._config.throwExceptionOnTimeout)
            .withNeededResponse(this._config.neededResponse)
            .build();
        const prevPendingRequest = this._pendingRequests.get(pendingRequest.id);
        if (prevPendingRequest) {
            logger.warn(`Pending Request was already exists for requestId ${pendingRequest.id}`);
        }
        this._pendingRequests.set(pendingRequest.id, pendingRequest);
        this._dispatchRequest(message, destinationEndpointIds);
        const tried = attempt ?? 0;
        return pendingRequest.then(responses => {
            return responses;
        }).catch(err => {
            if (tried < 3) {
                return this._request(message, targetEndpointIds, tried + 1);
            }
            throw err;
        });
    }

    protected abstract defaultResolvingEndpointIds(): ReadonlySet<string>;
    protected abstract sendNotification(message: Message): void;
    protected abstract sendRequest(message: Message): void;
    protected abstract sendResponse(message: Message): void;

    private _dispatch(
        message: Message, 
        forward: (message: Message) => void,
        destinationEndpointIds?: ReadonlySet<string>,
    ): void {
        message.storageId = this._config.topic;
        if (!destinationEndpointIds) {
            forward(message);
            return;
        } else if (destinationEndpointIds.size < 1) {
            logger.warn(`Empty set of destination has been provided for request`, message);
            return;
        }
        if (destinationEndpointIds.size === 1) {
            for (const destinationId of destinationEndpointIds) {
                message.destinationId = destinationId;
                forward(message);
            }
            return;
        }
        for (const destinationId of destinationEndpointIds) {
            forward(new Message({
                ...message,
                destinationId
            }));
        }
    }

    private _dispatchRequest(message: Message, targetEndpointIds?: ReadonlySet<string>): void {
        this._dispatch(
            message,
            this.sendRequest.bind(this),
            targetEndpointIds,
        );
    }

    private _dispatchResponse(message: Message, targetEndpointIds?: ReadonlySet<string>): void {
        for (const chunk of this._responseChunker.apply(message)) {
            this._dispatch(
                chunk,
                this.sendResponse.bind(this),
                targetEndpointIds
            );
        }
    }

    private _dispatchNotification(message: Message, destinationId?: string): void {
        if (!destinationId) {
            this._dispatch(
                message,
                this.sendNotification.bind(this),
            );
            return;
        }
        if (destinationId === this.localEndpointId || message.destinationId === this.localEndpointId) {
            // loopback notifications
            this.receive(message);
            return;
        }
        this._dispatch(
            message,
            this.sendNotification.bind(this),
            new Set<string>(destinationId)
        );
    }

    private _processResponse(message: Message): void {
        const chunkedResponse = message.sequence !== undefined && message.lastMessage !== undefined;
        const onlyOneChunkExists = message.sequence === 0 && message.lastMessage === true;
        if (chunkedResponse && !onlyOneChunkExists) {
            const pendingResponseId = `${message.sourceId}#${message.requestId}`;
            let pendingResponse = this._pendingResponses.get(pendingResponseId);
            if (!pendingResponse) {
                pendingResponse = new PendingResponse();
                this._pendingResponses.set(pendingResponseId, pendingResponse);
            }
            pendingResponse.accept(message);
            if (!pendingResponse.isReady) {
                return;
            }
            if (!this._pendingResponses.delete(pendingResponseId)) {
                logger.warn(`Unsuccessful deleting for pending response ${pendingResponseId}`);
            }
            const assembledResponse = pendingResponse.result;
            if (!assembledResponse) {
                logger.warn(`Undefined Assembled response, cannot make a response for request ${message.requestId}`, message);
                return;
            }
            message = assembledResponse;
        }
        if (!message.requestId) {
            logger.warn(`response message does not have a requestId`, message);
            return;
        }
        const pendingRequest = this._pendingRequests.get(message.requestId);
        if (!pendingRequest) {
            logger.warn(`Cannot find pending request for requestId ${message.requestId}`, message);
            return;
        }
        if (pendingRequest.isDone()) {
            logger.warn(`Response is received for an already done request. `, pendingRequest, message);
            return;
        }
        pendingRequest.accept(message);
    }

    private _createReceiver(): MessageProcessor<void> {
        const dispatchResponse = this._processResponse.bind(this);
        const codec = this._codec;
        const emitter = this._emitter;
        const result = new class extends MessageDefaultProcessor<void> {

            protected processAddSubscriptionRequest(message: Message): void {
                try {
                    const notification = codec.decodeAddSubscriptionRequest(message);
                    emitter.emit(ADD_SUBSCRIPTION_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processAddSubscriptionRequest(): Error occurred while decoding message", err)
                }
            }

            protected processAddSubscriptionResponse(message: Message): void {
                dispatchResponse(message);
            }

            protected processAddSubscriptionNotification(message: Message): void {
                try {
                    const notification = codec.decodeAddSubscriptionNotification(message);
                    emitter.emit(ADD_SUBSCRIPTION_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processAddSubscriptionNotification(): Error occurred while decoding message", err)
                }
            }

            protected processRemoveSubscriptionRequest(message: Message): void {
                try {
                    const notification = codec.decodeRemoveSubscriptionRequest(message);
                    emitter.emit(REMOVE_SUBSCRIPTION_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processRemoveSubscriptionRequest(): Error occurred while decoding message", err)
                }
            }

            protected processRemoveSubscriptionResponse(message: Message): void {
                dispatchResponse(message);
            }

            protected processRemoveSubscriptionNotification(message: Message): void {
                try {
                    const notification = codec.decodeRemoveSubscriptionNotification(message);
                    emitter.emit(REMOVE_SUBSCRIPTION_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processRemoveSubscriptionNotification(): Error occurred while decoding message", err)
                }
            }

            protected processPublishDataRequest(message: Message): void {
                try {
                    const notification = codec.decodePublishCustomDataRequest(message);
                    emitter.emit(PUBLISH_CUSTOM_DATA_REQUEST, notification);
                } catch (err) {
                    logger.warn("dispatcher::processPublishDataRequest(): Error occurred while decoding message", err)
                }
            }

            protected processPublishDataResponse(message: Message): void {
                dispatchResponse(message);
            }

            protected processPublishDataNotification(message: Message): void {
                try {
                    const notification = codec.decodePublishCustomDataNotification(message);
                    emitter.emit(PUBLISH_CUSTOM_DATA_NOTIFICATION, notification);
                } catch (err) {
                    logger.warn("dispatcher::processPublishDataNotification(): Error occurred while decoding message", err)
                }
            }

            protected processGetSubscriptionsRequest(message: Message) {
                try {
                    const request = codec.decodeGetSubscriptionsRequest(message);
                    emitter.emit(GET_SUBSCRIPTIONS_REQUEST, request);
                } catch (err) {
                    logger.warn("dispatcher::processGetSubscriptionsRequest(): Error occurred while decoding message", err)
                }
            }

            protected processGetSubscriptionsResponse(message: Message) {
                dispatchResponse(message);
            }

            protected processMessage(message: Message): void {
                logger.debug(`processMessage(): message type ${message.type} is not dispatched`);
            }

            protected processUnrecognizedMessage(message: Message): void {
                logger.warn(`processMessage(): message type ${message.type} is not recognized`);
            }
        }
        return result;
    }

}