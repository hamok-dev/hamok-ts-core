
export class FailedOperationException implements Error {
    readonly name = "FailedOperationException";
    public get message(): string {
        return this._message;
    }

    public get stack(): string | undefined {
        return this._stack;
    }


    private _message: string;
    private _stack?: string;
    public constructor(message?: string) {
        // this._stack = new Error().stack;
        this._message = message ?? "Failed Operation";
    }
}