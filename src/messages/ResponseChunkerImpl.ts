import { Message } from "@hamok-dev/common";
import { createPassiveChunker, ResponseChunker } from "./ResponseChunker";

export class ResponseChunkerImpl implements ResponseChunker{
    public static createPassiveChunker(): ResponseChunker {
        return createPassiveChunker();
    }

    private _maxKeys: number;
    private _maxEntries: number;

    public constructor(
        maxKeys: number,
        maxValues: number
    ) {
        this._maxKeys = maxKeys;
        this._maxEntries = Math.min(maxKeys, maxValues);
    }

    public *apply(message: Message): IterableIterator<Message> {
        if (message.keys === undefined || message.values === undefined) {
            yield message;
            return;
        }
        if (message.keys.length < 1) {
            yield message;
            return;
        }
        let iterator: IterableIterator<Message> | undefined;
        if (message.values.length < 1) {
            iterator = this._chunkByKeys(message);
        } else {
            iterator = this._chunkByEntries(message);
        }
        for (const chunk of iterator) {
            yield chunk;
        }
    }

    private *_chunkByKeys(message: Message): IterableIterator<Message> {
        if (message.keys.length <= this._maxKeys) {
            yield message;
            return;
        }
        const keys = message.keys;
        let sliceStart = 0;
        let sequence = 0;
        while (sliceStart < keys.length) {
            const sliceEnd = Math.min(sliceStart + this._maxKeys, keys.length);
            const lastMessage = keys.length === sliceEnd;
            yield new Message({
                ...message,
                keys: keys.slice(sliceStart, sliceEnd),
                sequence,
                lastMessage
            });
            sliceStart = sliceEnd;
            ++sequence;
        }
    }

    private *_chunkByEntries(message: Message): IterableIterator<Message> {
        if (Math.max(message.keys.length, message.values.length) <= this._maxEntries) {
            yield message;
            return;
        }
        const keys = message.keys;
        const values = message.values;
        let sliceStart = 0;
        let sequence = 0;
        while (sliceStart < keys.length && sliceStart < values.length) {
            const sliceEnd = Math.min(sliceStart + this._maxEntries, keys.length);
            const lastMessage = keys.length === sliceEnd;
            yield new Message({
                ...message,
                keys: keys.slice(sliceStart, sliceEnd),
                values: values.slice(sliceStart, sliceEnd),
                sequence,
                lastMessage
            });
            sliceStart = sliceEnd;
            ++sequence;
        }
    }
}