import { Message } from "@hamok-dev/common";

export interface LogEntry {
    readonly index: number;
    readonly term: number;
    readonly entry: Message;
    readonly timestamp: number;
}