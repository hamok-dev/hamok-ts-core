import { Message } from "@hamok-dev/common";
import { Message_MessageProtocol, Message_MessageType } from "@hamok-dev/common/lib/Models";
import { ResponseChunkerImpl } from "../../src/messages/ResponseChunkerImpl";
describe("ResponseChunkerImpl", () => {
    const responseChunker = new ResponseChunkerImpl(2, 1);
    describe("Chunk by key", () => {
        const createSourceMessage = (keys: Uint8Array[]) => {
            return new Message({
                requestId: "requestId",
                type: Message_MessageType.GET_ENTRIES_REQUEST,
                protocol: Message_MessageProtocol.GRID_COMMUNICATION_PROTOCOL,
                keys,
            });
        }

        it ("Chunk 1 key", () => {
            const key0 = `1`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(1);
            expect(chunks[0].type).toBe(original.type);
            expect(chunks[0].protocol).toBe(original.protocol);
            expect(chunks[0].requestId).toBe(original.requestId);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);

            expect(undefined).toBe(chunks[0].sequence);
            expect(undefined).toBe(chunks[0].lastMessage);
        });

        it ("Chunk 2 keys", () => {
            const key0 = `1`;
            const key1 = `2`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
                Buffer.from(key1, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(1);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].keys[1]).toString("utf-8")).toBe(key1);

            expect(undefined).toBe(chunks[0].sequence);
            expect(undefined).toBe(chunks[0].lastMessage);
        });

        it ("Chunk 3 keys", () => {
            const key0 = `1`;
            const key1 = `2`;
            const key2 = `3`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
                Buffer.from(key1, "utf-8"),
                Buffer.from(key2, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(2);
            expect(chunks[1].type).toBe(original.type);
            expect(chunks[1].protocol).toBe(original.protocol);
            expect(chunks[1].requestId).toBe(original.requestId);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].keys[1]).toString("utf-8")).toBe(key1);
            expect(Buffer.from(chunks[1].keys[0]).toString("utf-8")).toBe(key2);

            expect(0).toBe(chunks[0].sequence);
            expect(1).toBe(chunks[1].sequence);
            expect(false).toBe(chunks[0].lastMessage);
            expect(true).toBe(chunks[1].lastMessage);
        });

        it ("Chunk 4 keys", () => {
            const key0 = `1`;
            const key1 = `2`;
            const key2 = `3`;
            const key3 = `4`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
                Buffer.from(key1, "utf-8"),
                Buffer.from(key2, "utf-8"),
                Buffer.from(key3, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(2);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].keys[1]).toString("utf-8")).toBe(key1);
            expect(Buffer.from(chunks[1].keys[0]).toString("utf-8")).toBe(key2);
            expect(Buffer.from(chunks[1].keys[1]).toString("utf-8")).toBe(key3);

            expect(0).toBe(chunks[0].sequence);
            expect(1).toBe(chunks[1].sequence);
            expect(false).toBe(chunks[0].lastMessage);
            expect(true).toBe(chunks[1].lastMessage);
        });

        it ("Chunk 5 keys", () => {
            const key0 = `1`;
            const key1 = `2`;
            const key2 = `3`;
            const key3 = `4`;
            const key4 = `5`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
                Buffer.from(key1, "utf-8"),
                Buffer.from(key2, "utf-8"),
                Buffer.from(key3, "utf-8"),
                Buffer.from(key4, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(3);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].keys[1]).toString("utf-8")).toBe(key1);
            expect(Buffer.from(chunks[1].keys[0]).toString("utf-8")).toBe(key2);
            expect(Buffer.from(chunks[1].keys[1]).toString("utf-8")).toBe(key3);
            expect(Buffer.from(chunks[2].keys[0]).toString("utf-8")).toBe(key4);

            expect(0).toBe(chunks[0].sequence);
            expect(1).toBe(chunks[1].sequence);
            expect(2).toBe(chunks[2].sequence);
            expect(false).toBe(chunks[0].lastMessage);
            expect(false).toBe(chunks[1].lastMessage);
            expect(true).toBe(chunks[2].lastMessage);
        });
    });

    describe("Chunk by key", () => {
        const createSourceMessage = (keys: Uint8Array[], values: Uint8Array[]) => {
            return new Message({
                requestId: "requestId",
                type: Message_MessageType.GET_ENTRIES_REQUEST,
                protocol: Message_MessageProtocol.GRID_COMMUNICATION_PROTOCOL,
                keys,
                values,
            });
        };
        it ("Chunk 1 entries", () => {
            const key0 = `1`;
            const value0 = `value1`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
            ], [
                Buffer.from(value0, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(1);
            expect(chunks[0].type).toBe(original.type);
            expect(chunks[0].protocol).toBe(original.protocol);
            expect(chunks[0].requestId).toBe(original.requestId);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].values[0]).toString("utf-8")).toBe(value0);

            expect(undefined).toBe(chunks[0].sequence);
            expect(undefined).toBe(chunks[0].lastMessage);
        });

        it ("Chunk 2 entries", () => {
            const key0 = `1`;
            const key1 = `2`;
            const value0 = `value0`;
            const value1 = `value1`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
                Buffer.from(key1, "utf-8"),
            ], [
                Buffer.from(value0, "utf-8"),
                Buffer.from(value1, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(2);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].values[0]).toString("utf-8")).toBe(value0);
            expect(Buffer.from(chunks[1].keys[0]).toString("utf-8")).toBe(key1);
            expect(Buffer.from(chunks[1].values[0]).toString("utf-8")).toBe(value1);

            expect(0).toBe(chunks[0].sequence);
            expect(1).toBe(chunks[1].sequence);
            expect(false).toBe(chunks[0].lastMessage);
            expect(true).toBe(chunks[1].lastMessage);
        });

        it ("Chunk 3 entries", () => {
            const key0 = `1`;
            const key1 = `2`;
            const key2 = `3`;
            const value0 = `value0`;
            const value1 = `value1`;
            const value2 = `value2`;
            const original = createSourceMessage([
                Buffer.from(key0, "utf-8"),
                Buffer.from(key1, "utf-8"),
                Buffer.from(key2, "utf-8"),
            ], [
                Buffer.from(value0, "utf-8"),
                Buffer.from(value1, "utf-8"),
                Buffer.from(value2, "utf-8"),
            ]);
            const chunks = Array.from(responseChunker.apply(original));
            expect(chunks.length).toBe(3);
            expect(Buffer.from(chunks[0].keys[0]).toString("utf-8")).toBe(key0);
            expect(Buffer.from(chunks[0].values[0]).toString("utf-8")).toBe(value0);
            expect(Buffer.from(chunks[1].keys[0]).toString("utf-8")).toBe(key1);
            expect(Buffer.from(chunks[1].values[0]).toString("utf-8")).toBe(value1);
            expect(Buffer.from(chunks[2].keys[0]).toString("utf-8")).toBe(key2);
            expect(Buffer.from(chunks[2].values[0]).toString("utf-8")).toBe(value2);

            expect(0).toBe(chunks[0].sequence);
            expect(1).toBe(chunks[1].sequence);
            expect(2).toBe(chunks[2].sequence);
            expect(false).toBe(chunks[0].lastMessage);
            expect(false).toBe(chunks[1].lastMessage);
            expect(true).toBe(chunks[2].lastMessage);
        });
    });
    
});