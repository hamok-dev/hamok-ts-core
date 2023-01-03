import { 
    Message,
    MessageProtocol,
    MessageDefaultProcessor, 
    MessageProcessor 
} from "@hamok-dev/common"
import { GridDispatcher } from "./GridDispatcher";
import { GridTransport, GridTransportAbstract } from "./GridTransport"
import { ChangedLeaderListener, EndpointIdListener, Raccoon } from "./raccoons/Raccoon";


export class HamokGrid {
    private readonly _dispatcher = new GridDispatcher();
    private _transport: GridTransportAbstract;
    private _raccoon: Raccoon;

    private constructor(
        raccoon: Raccoon
    ) {
        this._raccoon = raccoon;
        this._transport = this._createTransport();
        this._dispatcher
            .onOutboundMessage(message => {
                message.protocol = MessageProtocol.GRID_COMMUNICATION_PROTOCOL;
                this._send(message);
            })
            .onInboundStorageSyncRequest(request => {
                // const response = request.createResponse();
                // this._dispatcher.emitOutboundStorageSyncResponse(response);
            })
        this._raccoon.onOutboundMessage(message => {
            message.protocol = MessageProtocol.RAFT_COMMUNICATION_PROTOCOL;
            this._send(message);
        });
        this._raccoon.start();
    }

    public get leaderId(): string | undefined {
        return this._raccoon.leaderId;
    }

    public get localEndpointId(): string {
        return this._raccoon.localPeerId;
    }

    public onRemoteEndpointJoined(listener: EndpointIdListener): this {
        this._raccoon.onJoinedRemotePeerId(listener);
        return this;
    }

    public onRemoteEndpointDetached(listener: EndpointIdListener): this {
        this._raccoon.onDetachedRemotePeerId(listener);
        return this;
    }

    public onLeaderChanged(listener: ChangedLeaderListener): this {
        this._raccoon.onChangedLeaderId(listener);
        return this;
    }

    public get transport(): GridTransport {
        return this._transport;
    }

    private _send(message: Message): void {
        message.sourceId = this._raccoon.localPeerId;
        this._transport.send(message);
    }

    private _createTransport(): GridTransportAbstract {
        const gridDispatcher = this._dispatcher;
        const raccoon = this._raccoon;
        return new class extends GridTransportAbstract {
            protected receive(message: Message): void {
                switch(message.protocol) {
                    case undefined:
                    case MessageProtocol.RAFT_COMMUNICATION_PROTOCOL:
                        raccoon.dispatchInboundMessage(message);
                        break;
                    case undefined:
                    case MessageProtocol.GRID_COMMUNICATION_PROTOCOL:
                        gridDispatcher.dispatchInboundMessage(message);
                        break;
                    
                }
                throw new Error("Method not implemented.");
            }
        };
    }

    public close(): void {
        this._raccoon.stop();
    }
}