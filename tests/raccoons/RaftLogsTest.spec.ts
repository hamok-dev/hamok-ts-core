import { Message } from "@hamok-dev/common";
import { LogEntry } from "../../src/raccoons/LogEntry";
import { RaftLogs } from "../../src/raccoons/RaftLogs";

describe("RaftLogs", () => {
    it ("When entry is submitted Then it can be collected", () => {
        const logs = new RaftLogs(new Map<number, LogEntry>());
        logs.submit(1, new Message({
            sourceId: "sourceId"
        }));
        const collectedEntries = logs.collectEntries(0);
        expect(collectedEntries[0].sourceId).toBe("sourceId");
    });

    it ("When entry is submitted Then it can be collected only if startIndex is not higher", () => {
        const logs = new RaftLogs(new Map<number, LogEntry>());
        logs.submit(1, new Message({
            sourceId: "sourceId"
        }));
        const collectedEntries = logs.collectEntries(1);
        expect(collectedEntries.length).toBe(0);
    });
});