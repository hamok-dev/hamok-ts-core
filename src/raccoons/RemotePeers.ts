import { 
    createLogger 
} from "@hamok-dev/common";
import { EventEmitter } from "events";

const logger = createLogger("RemotePeers");

const ON_REMOTE_PEER_JOINED_EVENT_NAME = "ON_REMOTE_PEER_JOINED_EVENT_NAME";
const ON_REMOTE_PEER_DETACHED_EVENT_NAME = "ON_REMOTE_PEER_DETACHED_EVENT_NAME";

export type RemoteEndpointStateChangedListener = (remoteEndpointId: string) => void;

export interface RemotePeer {
    id: string,
    touched: number
}

export class RemotePeers {

    private _emitter = new EventEmitter();
    private _remotePeers = new Map<string, RemotePeer>();
    private _activeRemotePeerIds = new Set<string>();
    private _changed = Date.now();

    public reset() {
        for (const peerId of Array.from(this._activeRemotePeerIds)) {
            this.detach(peerId);
        }
    }

    public detach(remotePeerId: string) {
        if (!this._remotePeers.delete(remotePeerId)) {
            logger.warn(`Cannot detach peer ${remotePeerId}, because it has not been registered as remote peer`);
            return;
        }

        this._updateRemotePeerIds();
        this._emitter.emit(ON_REMOTE_PEER_DETACHED_EVENT_NAME, remotePeerId);
    }

    public get(peerId: string): RemotePeer | undefined {
        return this._remotePeers.get(peerId);
    }

    public join(remotePeerId: string): void {
        if (this._activeRemotePeerIds.has(remotePeerId)) {
            logger.warn(`Attempted to join remote peer  ${remotePeerId} twice`);
            return;
        }
        const remotePeer: RemotePeer = {
            id: remotePeerId,
            touched: Date.now(),
        };
        this._remotePeers.set(remotePeerId, remotePeer);
        this._updateRemotePeerIds();
        this._emitter.emit(ON_REMOTE_PEER_JOINED_EVENT_NAME, remotePeerId);
    }

    public touch(remotePeerId: string) {
        const remotePeer = this._remotePeers.get(remotePeerId);
        if (remotePeer === undefined) {
            logger.warn(`Unknown remote peer ${remotePeerId} is attempted to be retrieved`)
            // this.join(remotePeerId);
            return;
        }
        this._remotePeers.set(remotePeerId, {
            id: remotePeerId,
            touched: Date.now(),
        });
    }

    public onRemoteEndpointJoined(listener: RemoteEndpointStateChangedListener): this {
        this._emitter.on(ON_REMOTE_PEER_JOINED_EVENT_NAME, listener);
        return this;
    }

    public offRemoteEndpointJoined(listener: RemoteEndpointStateChangedListener): this {
        this._emitter.off(ON_REMOTE_PEER_JOINED_EVENT_NAME, listener);
        return this;
    }

    public onRemoteEndpointDetached(listener: RemoteEndpointStateChangedListener): this {
        this._emitter.on(ON_REMOTE_PEER_DETACHED_EVENT_NAME, listener);
        return this;
    }

    public offRemoteEndpointDetached(listener: RemoteEndpointStateChangedListener): this {
        this._emitter.off(ON_REMOTE_PEER_DETACHED_EVENT_NAME, listener);
        return this;
    }

    public *[Symbol.iterator](): IterableIterator<RemotePeer> {
        for (const remotePeer of this._remotePeers.values()) {
            yield remotePeer;
        }
    }

    public get changed(): number {
        return this._changed;
    }

    public get size() {
        return this._activeRemotePeerIds.size;
    }

    public getRemotePeerIds(): ReadonlySet<string> {
        return this._activeRemotePeerIds;
    }

    private _updateRemotePeerIds(): void {
        this._activeRemotePeerIds = new Set(this._remotePeers.keys());
        this._changed = Date.now();
    }
}