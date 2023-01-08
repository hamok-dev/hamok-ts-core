import { Collections } from "@hamok-dev/common";
import { BatchIterator } from "../../src/distributedstorages/BatchIterator";

const createSource = (...entries: [number, string][]) => {
    return new Map<number, string>([...entries]);
}
const createGetBatch = (map: Map<number, string>) => {
    return async (keys: ReadonlySet<number>) => {
        const result = new Map<number, string>();
        for (const key of keys) {
            const value = map.get(key);
            if (value) {
                result.set(key, value);
            }
        }
        return result;
    }
}
describe("BatchIterator", () => {

    describe("iterate source", () => {
        const source = createSource([1, "one"], [2, "two"], [3, "three"]);
        const collect = async (batchIterator: BatchIterator<number, string>) => {
            const target = new Map<number, string>();
            for await (const entry of batchIterator.asyncIterator()) {
                target.set(...entry);
            }
            return target;
        }

        it ("iterate batches test 1", async () => {
            const batchIterator = new BatchIterator<number, string>(
                new Set<number>(source.keys()),
                createGetBatch(source),
                1
            );
            const target = await collect(batchIterator);
            expect(Collections.equalMaps(source, target)).toBe(true);
        });

        it ("iterate batches test 2", async () => {
            const batchIterator = new BatchIterator<number, string>(
                new Set<number>(source.keys()),
                createGetBatch(source),
                2
            );
            const target = await collect(batchIterator);
            expect(Collections.equalMaps(source, target)).toBe(true);
        });

        it ("iterate batches test 3", async () => {
            const batchIterator = new BatchIterator<number, string>(
                new Set<number>(source.keys()),
                createGetBatch(source),
                3
            );
            const target = await collect(batchIterator);
            expect(Collections.equalMaps(source, target)).toBe(true);
        });

        it ("iterate batches test 4", async () => {
            const batchIterator = new BatchIterator<number, string>(
                new Set<number>(source.keys()),
                createGetBatch(source),
                4
            );
            const target = await collect(batchIterator);
            expect(Collections.equalMaps(source, target)).toBe(true);
        });
    });
});