import { Message, setLogLevel } from "@hamok-dev/common";
import { Raccoon, RaccoonConfig, RaccoonImpl } from "../../src/raccoons/Raccoon";

setLogLevel("warn");

function createCluster(): [Raccoon, Raccoon, Raccoon] {
    const config: Partial<RaccoonConfig> = {
        followerMaxIdleInMs: 100,
        heartbeatInMs: 50
    };
    const alice = RaccoonImpl.builder().setConfig(config).build();
    const bob = RaccoonImpl.builder().setConfig(config).build();
    const eve = RaccoonImpl.builder().setConfig(config).build();

    alice.onOutboundMessage(bob.dispatchInboundMessage.bind(bob));
    alice.onOutboundMessage(eve.dispatchInboundMessage.bind(eve));

    bob.onOutboundMessage(alice.dispatchInboundMessage.bind(alice));
    bob.onOutboundMessage(eve.dispatchInboundMessage.bind(eve));

    eve.onOutboundMessage(alice.dispatchInboundMessage.bind(alice));
    eve.onOutboundMessage(bob.dispatchInboundMessage.bind(bob));

    alice.addRemotePeerId(bob.localPeerId);
    alice.addRemotePeerId(eve.localPeerId);
    bob.addRemotePeerId(alice.localPeerId);
    bob.addRemotePeerId(eve.localPeerId);
    eve.addRemotePeerId(alice.localPeerId);
    eve.addRemotePeerId(bob.localPeerId);

    return [alice, bob, eve];
}

describe("Raccoons", () => {
    describe("Two Raccoons communicate", () => {

        it ("they elect a leader", (done) => {
            const [alice, bob, eve] = createCluster();
            alice.onChangedLeaderId(leaderId => {
                alice.stop();
                bob.stop();
                eve.stop();
                done();
            });

            alice.start();
            bob.start();
            eve.start();
        });

        it ("only the leader accepts submits", (done) => {
            const [alice, bob, eve] = createCluster();
            alice.onChangedLeaderId(leaderId => {
                const message = new Message({ sourceId: "sourceId" });
                const submitted = 
                    (alice.submit(message) ? 1 : 0) +
                    (bob.submit(message) ? 1 : 0) +
                    (eve.submit(message) ? 1 : 0);
                alice.stop();
                bob.stop();
                eve.stop();
                expect(submitted).toBe(1);
                done();
            });

            alice.start();
            bob.start();
            eve.start();
        });

        it ("commit appears on all peers", (done) => {
            const [alice, bob, eve] = createCluster();
            let committed = 0;
            const commitDone = (num: number) => {
                committed = committed | num;
                if (committed < 7) return;
                alice.stop();
                bob.stop();
                eve.stop();
                done();
            }
            alice.onCommittedEntry((message) => {
                // console.warn("alice", message);
                expect(message.sourceId).toBe("sourceId");
                commitDone(1);
            })
            bob.onCommittedEntry((message) => {
                // console.warn("bob", message);
                expect(message.sourceId).toBe("sourceId");
                commitDone(2);
            })
            eve.onCommittedEntry((message) => {
                // console.warn("eve", message);
                expect(message.sourceId).toBe("sourceId");
                commitDone(4);
            })
            alice.onChangedLeaderId(leaderId => {
                const message = new Message({ sourceId: "sourceId" });
                alice.submit(message);
                bob.submit(message);
                eve.submit(message);
            });

            alice.start();
            bob.start();
            eve.start();
        });
    })
})