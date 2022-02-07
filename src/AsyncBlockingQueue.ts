// from https://stackoverflow.com/questions/47157428/how-to-implement-a-pseudo-blocking-async-queue-in-js-ts
export class AsyncBlockingQueue<T> {
  private _promises: Promise<T>[];
  private _resolvers: ((t: T) => void)[];

  constructor() {
    this._resolvers = [];
    this._promises = [];
  }

  private _add() {
    this._promises.push(new Promise(resolve => {
      this._resolvers.push(resolve);
    }));
  }

  enqueue(t: T) {
    if (!this._resolvers.length) this._add();
    const resolve = this._resolvers.shift()!;
    resolve(t);
  }

  dequeue(): Promise<T> {
    if (!this._promises.length) this._add();
    const promise = this._promises.shift()!;
    return promise;
  }

  isEmpty() {
    return !this._promises.length;
  }

  isBlocked() {
    return !!this._resolvers.length;
  }

  get length() {
    return this._promises.length - this._resolvers.length;
  }
}
