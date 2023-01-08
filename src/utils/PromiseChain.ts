
// export class PromiseChain<T> {
//     private _promises = new Map<number, Promise<T>>();
//     private _index = -1;

//     public add(createPromise: () => Promise<T>): Promise<T> {
//         const actualBlockingPoint = this._promises.get(this._index);
//         const index = ++this._index;
//         const result = new Promise<T>((resolve) => {
//             if (!actualBlockingPoint) {
//                 createPromise().finally(() => {
//                     this._promises.delete(index);
//                     resolve();
//                 });
//                 return;
//             }
//             actualBlockingPoint.finally(() => {
//                 createPromise().finally(() => {
//                     this._promises.delete(index);
//                 });
//             });
//         });
//         this._promises.set(index, result);
//         return result;
//     }

// }