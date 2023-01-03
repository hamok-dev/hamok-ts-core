
export enum CompletablePromiseState {
    REJECTED,
    RESOLVED,
    PENDING,
}

export class CompletablePromise<T = any> implements Promise<T> {
    private _promise: Promise<T>;
    private _resolve: (value: T) => void = () => undefined;
    private _reject: (error?: Error) => void = () => undefined;
    private _state: CompletablePromiseState;

    public constructor() {
        const that = this;
        this._promise = new Promise((resolve, reject) => {
            that._resolve = (data) => {
                this._state = CompletablePromiseState.RESOLVED;
                resolve(data);
            };
            that._reject = (error) => {
                this._state = CompletablePromiseState.REJECTED;
                reject(error);
            };
        });
        this._state = CompletablePromiseState.PENDING;
    }

    public then<TResult1 = T, TResult2 = never>(
        onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
    ): Promise<TResult1 | TResult2> {
        return this._promise.then(onfulfilled, onrejected);
    }

    public catch<TResult = never>(
        onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null
    ): Promise<T | TResult> {
        return this._promise.catch(onrejected);
    }

    public get [Symbol.toStringTag](): string {
        return this._promise[Symbol.toStringTag];
    }

    public finally(onfinally?: (() => void) | null): Promise<T> {
        return this._promise.finally(onfinally);
    }

    public get state() {
        return this._state;
    }

    public resolve(data: T): void {
        if (this._state !== CompletablePromiseState.PENDING) {
            throw new Error(`Promise cannot be rejected, because it is already completed`);
        }
        this._resolve(data);
    }

    public reject(error?: Error): void {
        if (this._state !== CompletablePromiseState.PENDING) {
            throw new Error(`Promise cannot be rejected, because it is already completed`);
        }
        this._reject(error);
    }
}