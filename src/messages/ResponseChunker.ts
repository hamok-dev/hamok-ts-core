import { Message } from "@hamok-dev/common";

export interface ResponseChunker {
    apply(message: Message): IterableIterator<Message>
}

export function createPassiveChunker(): ResponseChunker {
    return new class implements ResponseChunker {
        public *apply(message: Message): IterableIterator<Message> {
            yield message;
        }
    }
}
