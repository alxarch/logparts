import {List} from "immutable"
import {debuglog} from "util"
const debug = debuglog("db")
import {LevelUp} from "levelup";
import { AbstractLevelDOWN, AbstractBatch, AbstractIteratorOptions} from "abstract-leveldown";
import { MetroHash64 } from "metrohash";
import { ParsedUrlQuery } from "querystring";
type Keys<T> = (keyof T)[]
type Dict<T> = {[K: string]: T}
type Option<T> = T|undefined
type Config<T> = Readonly<Partial<T>>
type Level<T> = LevelUp<AbstractLevelDOWN<Buffer,T>>

const indexOf = Array.prototype.indexOf

// Maps partitions to DB instances
const lookup = new WeakMap()

export class DB<P,T=any> {
    private _partitions: Dict<Log<P,T>> = {}
    private _db: Level<T>
    private _loading: Dict<Promise<Log<P,T>>> = {}
    readonly keys: ReadonlyArray<keyof P>
    readonly options: Readonly<Options<P>>
    constructor(db: Level<T>, keys: Keys<P>, options: Config<Options<P>> = {}) {
        this._db = db
        this.options = {
            keyPrefix: defaultKeyPrefix,
            hasher: defaultHasher,
            ...options
        }
        this.keys = [...keys]
    }
    batch(batch: Batch<T>, options?: any): Promise<void> {
        return this._db.batch(batch, {...options, keyEncoding: 'binary', valueEncoding: 'json'})
    }
    hashValues(p: P, nonce = "") :string {
        return this.options.hasher(p, this.keys, nonce)
    }
    scan(options: IteratorOptions, scanner: Scanner<T>): Promise<void> {
        return new Promise((resolve, reject) => {
            const s = this._db.createReadStream({...options, keyAsBuffer: true, keyEncoding: 'binary'})
            s.on('data', scanner)
            s.on('error', reject)
            s.on('end', resolve)
        })
    }
    /**
     * Finds or creates a partition for the specified values.
     * 
     * Tries `DB.maxCollisions` times to resolve hash collisions
     * @param values values for the partition
     */
    async partition(values :P) :Promise<Option<Log<P, T>>> {
        let nonce = ""
        let retries = maxCollisions
        while(retries--) {
            const id = this.hashValues(values, nonce)
            let partition = this._partitions[id]
            if (partition === undefined) {
                partition = await this._loadOnce(id, values)
            } 
            if (this.equal(partition!.values, values)) {
                return partition
            }
            debug("Partition collision", id, values, this._partitions[id]!.values)
            nonce = String(maxCollisions - retries)
        }
        debug("Max partition collisions occured")
        return
    }

    private _addPartition(id :string, values: P) :Log<P,T> {
        values = this._partitionValues(values)
        const p = new Log<P,T>(id, values)
        lookup.set(p, this)
        this._partitions[id] = p
        return p
    }
    match(q: ParsedUrlQuery, p: P) :boolean {
        let i = this.keys.length
        while(i--) {
            const k = this.keys[i]
            if (k in q) {
                const want = q[k as string]
                const v = String(p[k])
                if (typeof want === 'string' ? want === v : want.indexOf(v) !== -1) {
                    continue
                }
            }
            return false
        }
        return true
    }
    matchLogs(q: ParsedUrlQuery) :Log<P,T>[] {
        const logs :Log<P,T>[] = []
        debug("Partitions", Object.keys(this._partitions))
        for (let id in this._partitions) {
            const p = this._partitions[id]
            debug("P", p.values, q)
            if (this.match(q, p.values)) {
                logs.push(p)
            }
        }
        debug("Matched partitions", q, logs.length)
        return logs
    }
    equal(a :P, b :P) :boolean {
        let i = this.keys.length
        while(i--) {
            const k = this.keys[i]
            if (a[k] !== b[k]) {
                return false
            }
        }
        return true
    }
    partitionKey(id :string) :Buffer {
        const size = this.options.keyPrefix.length + 'partition'.length+id.length+2
        const buf = Buffer.alloc(size)
        let n = buf.write(this.options.keyPrefix)
        n = buf.writeUInt8(':'.charCodeAt(0), n)
        n += buf.write('partition', n)
        n = buf.writeUInt8(':'.charCodeAt(0), n)
        n += buf.write(id, n)
        return buf.slice(0, n)
    }

    private _partitionValues(values :P) :P {
        const p = {} as P
        let i = this.keys.length
        while(i--) {
            const k = this.keys[i]
            const v = values[k]
            p[k] = v
        }
        return p
    }

    /**
     * Load or create a partition in db
     * 
     * @param id the partition id
     * @param values the partition's values
     */
    private async _load(id: string, values: P) :Promise<Log<P,T>> {
        // Cast db as storing P values
        const db = this._db as unknown as LevelUp<AbstractLevelDOWN<Buffer,P>>
        const key = this.partitionKey(id)
        try {
            values = await db.get(key, {
                valueEncoding: 'json'
            })
            return this._addPartition(id, values)
        } catch (err) {
            if (err.notFound) {
                const p = this._addPartition(id, values)
                await db.put(key, p.values, {
                    valueEncoding: 'json',
                })
                return p
            }
            throw err
        }
    }

    /**
     * Load partition from the db once per id
     */
    private async _loadOnce(id: string, values: P) :Promise<Log<P, T>> {
        let p = this._loading[id]
        if (p === undefined) {
            p = this._load(id, values)
            this._loading[id] = p
            try {
                return await p
            } finally {
                delete(this._loading[id])
            }
        } else {
            return p
        }
    }
}


export interface ScanResult<T> {
    key: Buffer,
    value: T,
}
export type Scanner<T> = (r: ScanResult<T>) => void
export type IteratorOptions = AbstractIteratorOptions<Buffer>
export type Hasher<T> = (values: T, keys: ReadonlyArray<keyof T>, nonce: string) => string

export interface Options<P> {
    keyPrefix: string
    hasher: Hasher<P>,
}
export function defaultHasher<T>(v: T, keys: ReadonlyArray<keyof T>, nonce: string) :string {
    const m = new MetroHash64()
    for(let k of keys) {
        m.update(String(v[k]))
    }
    m.update(nonce)
    return m.digest()
}
export const defaultKeyPrefix = "db"
export type Batch<T> = AbstractBatch<Buffer, T>[]
interface Entry<T> {
    value: T,
    ts: number,
}
type Entries<T> = List<Entry<T>>

export interface Stats {
    hit: number
    miss: number
    fill: number
    flush: number
    batch: number
}

export class Log<P,T> {
    readonly id: string
    private _cache: Entries<T> = List()
    private _syncing?: Promise<void>
    private _maxT = 0
    private _next = 0
    private _batch: Batch<T> = []
    private _stats: Stats = {
        hit: 0,
        miss: 0,
        fill: 0,
        flush: 0,
        batch: 0,
    }
    readonly values: Readonly<P>
    constructor(id: string, values: P) {
        this.id = id
        this.values = values
    }
    get stats() :Readonly<Stats> {
        return this._stats
    }
    async shift(now: Date) :Promise<Option<T>> {
        let result = this._cache.first(undefined)
        if (result === undefined) {
            const db = this.db
            this._stats.miss++
            await this._sync(db, now)
            result = this._cache.first()
            if (result === undefined) {
                return undefined
            }
        } else {
            this._stats.hit++
        }
        this._cache = this._cache.shift()
        return result.value
    }
    push(value: T, ts: Date) {
        const db = this.db
        this._batch.push({
            type: 'put',
            key: this.keyBuffer(db.options.keyPrefix, ts),
            value: value,
        })
    }

    private fill(db: DB<P,T>, now: Date) :Promise<void> {
        const options = {
            gt: this.keyBuffer(db.options.keyPrefix, new Date(this._maxT)),
            lt: this.keyBuffer(db.options.keyPrefix, now),
        }
        return db.scan(options, (r: ScanResult<T>) => {
            const ts = keyTimestamp(r.key)
            this._cache = this._cache.push({
                value: r.value,
                ts: ts,
            })
            this._batch.push({
                type: 'del',
                key: r.key,
            })
            this._maxT = ts
        })
    }
    sync(now: Date) :Promise<void> {
        return this._sync(this.db, now)
    }
    private async _sync(db: DB<P,T>, now: Date) :Promise<void> {
        if (this._syncing === undefined) {
            this._syncing = (async () => {
                try {
                    const size = this._batch.length
                    await this.flush(db)
                    this._stats.flush++
                    this._stats.batch += size
                    await this.fill(db, now)
                    this._stats.fill++
                } finally {
                    this._syncing = undefined
                }
            })()
        } 
        return this._syncing
    }

    private flush(db: DB<P,T>) :Promise<void> {
        const batch = this._batch
        this._batch = []
        return db.batch(batch, { valueEncoding: 'json'})
            .catch(err => {
                this._batch = batch.concat(this._batch)
                throw err
            })
    }

    get db() :DB<P,T> {
        const db = lookup.get(this)
        if (db === undefined) {
            throw new Error('No db for log')
        }
        return db
    }
    private keyBuffer(prefix: string, ts: Date) :Buffer {
        const buf = Buffer.allocUnsafe(prefix.length+25) // 0x00, 8-byte partition id, 8-byte timestamp, 8-byte id
        // Write prefix and add '\0' for correct lexicographical order
        let n = buf.write(prefix, 0)
        n = buf.writeUInt8(0, n)
        // Write the partition id as binary (id is the hex string of 8 bytes)
        n += buf.write(this.id, n, 8, 'hex')
        n = buf.writeDoubleBE(ts.getTime(), n)
        const id = this._next++
        n = buf.writeDoubleBE(id, n)
        return buf.slice(0, n)
    }
}

function keyTimestamp(k: Buffer) :number {
    return k.readDoubleBE(k.length - 16)
}
export const maxCollisions = 8
