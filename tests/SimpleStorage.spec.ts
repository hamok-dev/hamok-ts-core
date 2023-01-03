import { SimpleStorage } from "../src/SimpleStorage";
import { SimpleSlidingMap } from "../src/SlidingMap";

describe("SimpleStorage", () => {
    const createStorage = () => SimpleStorage.builder<number, string>()
        .setId("storageId")
        .build();
    it("cannot build without id", () => {
        expect(() => SimpleStorage.builder<number, string>().build()).toThrowError();
    });

    describe("Specification", () => {
        it("set / get", async () => {
            const storage = createStorage();
            
            await storage.set(1, "one");
            expect(await storage.get(1)).toBe("one");
        });

        it("setAll / getAll", async () => {
            const storage = createStorage();
            
            await storage.setAll(new Map<number, string>([[1, "one"], [2, "two"]]));
            const entries = await storage.getAll(new Set<number>([1, 2]));
            expect(entries.get(1)).toBe("one");
            expect(entries.get(2)).toBe("two");
        });

        it("restore / get", async () => {
            const storage = createStorage();
            
            await storage.restore(1, "one");
            expect(await storage.get(1)).toBe("one");
        });

        it("cannot restore already set entry", (done) => {
            const storage = createStorage();
            
            storage.set(1, "one");
            expect(() => storage.restore(1, "two")).toThrowError();
        });

        it("restoreAll / getAll", async () => {
            const storage = createStorage();
            
            await storage.restoreAll(new Map<number, string>([[1, "one"], [2, "two"]]));
            const entries = await storage.getAll(new Set<number>([1, 2]));
            expect(entries.get(1)).toBe("one");
            expect(entries.get(2)).toBe("two");
        });

        it("cannot restore already set entry", (done) => {
            const storage = createStorage();
            
            storage.setAll(new Map<number, string>([[1, "one"], [2, "two"]]))
                .then(() => storage.restoreAll(new Map<number, string>([[1, "one"]])))
                .catch(err => done());
        });
        

        it("set / delete", async () => {
            const storage = createStorage();
            
            await storage.set(1, "one");
            await storage.delete(1);
            expect(await storage.get(1)).toBe(undefined);
        });

        it("setAll / deleteAll", async () => {
            const storage = createStorage();
            
            await storage.setAll(new Map<number, string>([[1, "one"], [2, "two"]]));
            await storage.deleteAll(new Set<number>([1]));
            const entries = await storage.getAll(new Set<number>([1, 2]));
            expect(entries.get(1)).toBe(undefined);
            expect(entries.get(2)).toBe("two");
        });

        it("set / evict", async () => {
            const storage = createStorage();
            
            await storage.set(1, "one");
            await storage.evict(1);
            expect(await storage.get(1)).toBe(undefined);
        });

        it("setAll / evictAll", async () => {
            const storage = createStorage();
            
            await storage.setAll(new Map<number, string>([[1, "one"], [2, "two"]]));
            await storage.evictAll(new Set<number>([1]));
            const entries = await storage.getAll(new Set<number>([1, 2]));
            expect(entries.get(1)).toBe(undefined);
            expect(entries.get(2)).toBe("two");
        });
    });

    describe("events", () => {
        it("When entry is created Then event is emitted", (done) => {
            const storage = createStorage();
            storage.events.onCreatedEntry(event => {
                expect(event.key).toBe(1);
                expect(event.value).toBe("one");
                done();
            });
            storage.set(1, "one");
        });

        it("When entry is updated Then event is emitted", (done) => {
            const storage = createStorage();
            storage.set(1, "one");

            storage.events.onUpdatedEntry(event => {
                expect(event.key).toBe(1);
                expect(event.oldValue).toBe("one");
                expect(event.newValue).toBe("updatedOne");
                done();
            });
            storage.set(1, "updatedOne");
        });

        it("When entry is deleted Then event is emitted", (done) => {
            const storage = createStorage();
            storage.set(1, "one");

            storage.events.onDeletedEntry(event => {
                expect(event.key).toBe(1);
                expect(event.value).toBe("one");
                done();
            });
            storage.delete(1);
        });

        it("When entry is evicted Then event is emitted", (done) => {
            const storage = createStorage();
            storage.set(1, "one");

            storage.events.onEvictedEntry(event => {
                expect(event.key).toBe(1);
                expect(event.value).toBe("one");
                done();
            });
            storage.evict(1);
        });

        it("When entry is restored Then event is emitted", (done) => {
            const storage = createStorage();
            storage.events.onRestoredEntry(event => {
                expect(event.key).toBe(1);
                expect(event.value).toBe("one");
                done();
            });
            storage.restore(1, "one");
        });

        it("When entry is expired Then event is emitted", (done) => {
            const storage = SimpleStorage.builder<number, string>()
                .setId("storageId")
                .setSlidingMap(
                    SimpleSlidingMap
                        .builder<number, string>()
                        .setExpirationTime(100, true, 10)
                        .build()
                )
                .build();
            storage.set(1, "one")
            storage.events.onExpiredEntry(event => {
                expect(event.key).toBe(1);
                expect(event.value).toBe("one");
                done();
            });
        });
    });
});