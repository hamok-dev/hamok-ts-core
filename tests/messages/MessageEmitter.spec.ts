import { MessageEmitter } from "../../src/messages/MessageEmitter";

describe("MessageEmitter", () => {
    let activeListeners = 0;
    const createPassingListener = (timeoutInMs: number) => async (value: string) => {
        if (++activeListeners !== 1) {
            throw new Error(`Failed test, activeListeners: ${activeListeners}`);
        }
        return new Promise<void>(resolve => {
            setTimeout(() => {
                --activeListeners;
                resolve();
            }, timeoutInMs);
        });
    };
    const createFailingListener = (timeoutInMs: number) => async (value: string) => {
        if (++activeListeners !== 1) throw new Error(`Failed test`);
        return new Promise<void>((resolve, reject) => {
            setTimeout(() => {
                --activeListeners;
                reject();
            }, timeoutInMs);
        });
    };

    it("block executions", async () => {
        const emitter = new MessageEmitter();
        emitter.addBlockingListener("event1", createPassingListener(200));
        emitter.addBlockingListener("event2", createPassingListener(200));
        emitter.addBlockingListener("event2", createFailingListener(200));
        const started = Date.now();
        emitter.emit("event1").emit("event2").emit("event1");
        // emitter.emit("event1").emit("event1");
        expect(emitter.queueSize).toBe(4);
        await emitter.actualBlockingPoint;
        expect(emitter.queueSize).toBe(0);
        expect(Date.now() - started).toBeGreaterThanOrEqual(800);
        emitter.removeAllListeners();
    });

    it("remove listeners", async () => {
        const emitter = new MessageEmitter();
        const listener = createPassingListener(200);
        emitter.addBlockingListener("event1", createPassingListener(200));
        emitter.addBlockingListener("event2", listener);
        emitter.addBlockingListener("event2", createFailingListener(200));
        emitter.removeListener("event2", listener);
        const started = Date.now();
        emitter.emit("event1").emit("event2").emit("event1");
        // emitter.emit("event1").emit("event1");
        expect(emitter.queueSize).toBe(3);
        await emitter.actualBlockingPoint;
        expect(emitter.queueSize).toBe(0);
        expect(Date.now() - started).toBeGreaterThanOrEqual(600);
        emitter.removeAllListeners();
    });
});