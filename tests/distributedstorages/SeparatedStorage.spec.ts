import { HamokGrid } from "../../src/HamokGrid";
import { SimpleStorage } from "../../src/storages/SimpleStorage";
describe("SeparatedStorage", () => {
    const usEast = HamokGrid.builder().build();
    const euWest = HamokGrid.builder().build();

    usEast.transport.sender = euWest.transport.receiver;
    euWest.transport.sender = usEast.transport.receiver;

    usEast.addRemoteEndpointId(euWest.localEndpointId);
    euWest.addRemoteEndpointId(usEast.localEndpointId);
    
    Promise.all([
        usEast.start(),
        euWest.start(),
    ]).then(() => {
        console.log("Started");
    });
    const numEncoder = (num: number) => new Uint8Array([num]);
    const numDecoder = (bytes: Uint8Array) => bytes[0];
    const strEncoder = (data: string) => Buffer.from(data, "utf-8");
    const strDecoder = (data: Uint8Array) => Buffer.from(data).toString("utf-8");
    const euSeparatedStorage = euWest.createSeparatedStorage<number, string>(
        SimpleStorage.builder<number, string>()
            .setId("SEPARATED_STORAGE")
            .build()
    )
        .withKeyEncoder(numEncoder)
        .withKeyDecoder(numDecoder)
        .withValueEncoder(strEncoder)
        .withValueDecoder(strDecoder)
        .build();
    const usSeparatedStorage = usEast.createSeparatedStorage<number, string>(
        SimpleStorage.builder<number, string>()
            .setId("SEPARATED_STORAGE")
            .build()
    )
        .withKeyEncoder(numEncoder)
        .withKeyDecoder(numDecoder)
        .withValueEncoder(strEncoder)
        .withValueDecoder(strDecoder)
        .build();
    it("set / get", async () => {
        await euSeparatedStorage.set(1, "one");
        console.warn(await usSeparatedStorage.get(1));
    });

    usEast.stop();
    euWest.stop();

});