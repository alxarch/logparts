"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const immutable_1 = require("immutable");
const metrohash_1 = require("metrohash");
const util_1 = require("util");
const debug = util_1.debuglog("logparts");
// Maps partitions to DB instances
const lookup = new WeakMap();
const minPartitionId = '0000000000000000';
const maxPartitionId = 'ffffffffffffffff';
class Log {
    constructor(db, keys, keyPrefix = 'log') {
        this._partitions = {};
        this._loading = {};
        this._db = db;
        this.keyPrefix = keyPrefix;
        this.keys = [...keys];
    }
    batch(batch, options = {}) {
        return this._db.batch(batch, Object.assign({ valueEncoding: 'json' }, options, { keyEncoding: 'binary' }));
    }
    hashValues(p, nonce = "0") {
        const m = new metrohash_1.MetroHash64();
        for (let k of this.keys) {
            m.update(nonce);
            m.update(String(p[k]));
        }
        return m.digest();
    }
    scan(options, scanner) {
        return new Promise((resolve, reject) => {
            const s = this._db.createReadStream(Object.assign({ valueEncoding: 'json' }, options, { keyAsBuffer: true, keyEncoding: 'binary' }));
            s.on('data', scanner);
            s.on('error', reject);
            s.on('end', resolve);
        });
    }
    /**
     * Finds or creates a partition for the specified values.
     *
     * Tries `DB.maxCollisions` times to resolve hash collisions
     * @param values values for the partition
     */
    async partition(values) {
        let nonce = "";
        let retries = exports.maxCollisions;
        while (retries--) {
            const id = this.hashValues(values, nonce);
            let partition = this._partitions[id];
            if (partition === undefined) {
                partition = await this._loadOnce(id, values);
            }
            if (this.equal(partition.values, values)) {
                return partition;
            }
            debug("Partition collision", id, values, this._partitions[id].values);
            nonce = String(exports.maxCollisions - retries);
        }
        debug("Max partition collisions occured");
        return;
    }
    _addPartition(id, values) {
        values = this._partitionValues(values);
        const p = new Partition(id, values);
        lookup.set(p, this);
        this._partitions[id] = p;
        return p;
    }
    match(q, p) {
        let i = this.keys.length;
        while (i--) {
            const k = this.keys[i];
            if (k in q) {
                const want = q[k];
                const v = String(p[k]);
                if (typeof want === 'string' ? want === v : want.indexOf(v) !== -1) {
                    continue;
                }
            }
            return false;
        }
        return true;
    }
    equal(a, b) {
        let i = this.keys.length;
        while (i--) {
            const k = this.keys[i];
            if (a[k] !== b[k]) {
                return false;
            }
        }
        return true;
    }
    _matchAll(q) {
        const logs = [];
        debug("Partitions", Object.keys(this._partitions));
        for (let id in this._partitions) {
            const p = this._partitions[id];
            if (this.match(q, p.values)) {
                logs.push(p);
            }
        }
        debug("Matched partitions", q, logs.length);
        return logs;
    }
    async summary() {
        const [[entries, minK, maxK], [partitions]] = await Promise.all([
            this._count(this.minEntryKey, this.maxEntryKey),
            this._count(this.minPartitionKey, this.maxPartitionKey),
        ]);
        const min = new Date(keyTimestamp(minK));
        const max = new Date(keyTimestamp(maxK));
        return { entries, min, max, partitions };
    }
    async count(q) {
        const partitions = this._matchAll(q);
        const tasks = [];
        let hit = 0;
        let miss = 0;
        let flush = 0;
        let fill = 0;
        let batch = 0;
        let pending = 0;
        for (let p of partitions) {
            const minKey = this.entryKey(p.id, 0, 0);
            const maxKey = this.entryKey(p.id, Number.MAX_SAFE_INTEGER, MAX_UINT_32);
            tasks.push(this._count(minKey, maxKey));
            hit += p.stats.hit;
            miss += p.stats.miss;
            fill += p.stats.fill;
            flush += p.stats.flush;
            batch += p.stats.batch;
            pending += p.batch.length;
        }
        const results = await Promise.all(tasks);
        let total = 0;
        let min = Number.MAX_SAFE_INTEGER;
        let max = Number.MIN_SAFE_INTEGER;
        for (let [n, minK, maxK] of results) {
            if (n === 0) {
                continue;
            }
            total += n;
            n = keyTimestamp(minK);
            if (n < min) {
                min = n;
            }
            n = keyTimestamp(maxK);
            if (n > max) {
                min = n;
            }
        }
        return {
            entries: total,
            min: new Date(min),
            max: new Date(max),
            partitions: partitions.length,
            hit,
            miss,
            batch,
            fill,
            flush,
            pending,
        };
    }
    partitionKey(id) {
        const buf = Buffer.alloc(this.partitionKeySize);
        let n = buf.write(this.keyPrefix);
        n = buf.writeUInt8(0, n);
        n += buf.write('partition', n);
        n = buf.writeUInt8(0, n);
        n += buf.write(id, n, 8, 'hex');
        return buf.slice(0, n);
    }
    get partitionKeySize() {
        return this.keyPrefix.length + 1 + 'partition'.length + 1 + 8;
    }
    get minPartitionKey() {
        return this.partitionKey(minPartitionId);
    }
    get maxPartitionKey() {
        return this.partitionKey(maxPartitionId);
    }
    entryKey(logId, ts, id = 0) {
        const buf = Buffer.allocUnsafe(this.entryKeySize);
        // Write prefix and add '\0' for correct lexicographical order
        let n = buf.write(this.keyPrefix, 0);
        n = buf.writeUInt8(0, n);
        // Write the partition id as binary (id is the hex string of 8 bytes)
        n += buf.write(logId, n, 8, 'hex');
        n = writeTimestampBE(buf, ts, n);
        n = buf.writeUInt32BE(id, n);
        return buf.slice(0, n);
    }
    get entryKeySize() {
        // prefix + 0x00, 8-byte partition id, 8-byte timestamp, 4-byte id
        return this.keyPrefix.length + 1 + 8 + 8 + 4;
    }
    get minEntryKey() {
        return this.entryKey(minPartitionId, 0);
    }
    get maxEntryKey() {
        return this.entryKey(maxPartitionId, Number.MAX_SAFE_INTEGER, MAX_UINT_32);
    }
    _count(min, max) {
        let total = 0;
        return new Promise((resolve, reject) => {
            const s = this._db.createKeyStream({
                keyAsBuffer: true,
                keyEncoding: 'binary',
            });
            s.on('error', reject);
            s.on('data', key => {
                min = total === 0 ? key : min;
                max = key;
                total++;
            });
            s.on('end', () => {
                resolve([total, min, max]);
            });
        });
    }
    _partitionValues(values) {
        const p = {};
        let i = this.keys.length;
        while (i--) {
            const k = this.keys[i];
            const v = values[k];
            p[k] = v;
        }
        return p;
    }
    /**
     * Load or create a partition in db
     *
     * @param id the partition id
     * @param values the partition's values
     */
    async _load(id, values) {
        // Cast db as storing P values
        const db = this._db;
        const key = this.partitionKey(id);
        try {
            values = await db.get(key, {
                valueEncoding: 'json'
            });
            return this._addPartition(id, values);
        }
        catch (err) {
            if (err.notFound) {
                const p = this._addPartition(id, values);
                await db.put(key, p.values, {
                    valueEncoding: 'json',
                });
                return p;
            }
            throw err;
        }
    }
    /**
     * Load partition from the db once per id
     */
    async _loadOnce(id, values) {
        let p = this._loading[id];
        if (p === undefined) {
            p = this._load(id, values);
            this._loading[id] = p;
            try {
                return await p;
            }
            finally {
                delete (this._loading[id]);
            }
        }
        else {
            return p;
        }
    }
}
exports.Log = Log;
exports.defaultKeyPrefix = "db";
class Partition {
    constructor(id, values) {
        this._cache = immutable_1.List();
        this._maxT = 0;
        this._next = 0;
        this._batch = [];
        this._stats = {
            hit: 0,
            miss: 0,
            fill: 0,
            flush: 0,
            batch: 0,
        };
        this.id = id;
        this.values = values;
    }
    get nextId() {
        const id = this._next;
        this._next = id === MAX_UINT_32 ? 0 : id + 1;
        return id;
    }
    get batch() {
        return this._batch;
    }
    get stats() {
        return this._stats;
    }
    get maxDate() {
        return new Date(this._maxT);
    }
    async shift(now) {
        let result = this._cache.first(undefined);
        if (result === undefined) {
            const db = this.db;
            this._stats.miss++;
            await this._sync(db, now);
            result = this._cache.first();
            if (result === undefined) {
                return undefined;
            }
        }
        else {
            this._stats.hit++;
        }
        this._batch.push({
            type: 'del',
            key: result.key,
        });
        this._cache = this._cache.shift();
        return result.value;
    }
    push(value, ts) {
        if (ts.getTime() <= this._maxT) {
            throw new Error('Timestamp in the past');
        }
        const db = this.db;
        this._batch.push({
            type: 'put',
            key: db.entryKey(this.id, ts.getTime(), this.nextId),
            value: value,
        });
    }
    async close() {
        if (lookup.has(this)) {
            const db = lookup.get(this);
            try {
                db.batch(this._batch);
            }
            catch (err) {
                throw err;
            }
            finally {
                lookup.delete(this);
            }
        }
    }
    fill(db, now) {
        const options = {
            gt: db.entryKey(this.id, this._maxT),
            lte: db.entryKey(this.id, now.getTime(), MAX_UINT_32),
        };
        return db.scan(options, (r) => {
            const ts = keyTimestamp(r.key);
            this._cache = this._cache.push({
                value: r.value,
                ts: ts,
                key: r.key,
            });
            this._maxT = ts;
        });
    }
    sync(now) {
        return this._sync(this.db, now);
    }
    async _sync(db, now) {
        if (this._syncing === undefined) {
            this._syncing = (async () => {
                try {
                    const size = this._batch.length;
                    await this.flush(db);
                    this._stats.flush++;
                    this._stats.batch += size;
                    await this.fill(db, now);
                    this._stats.fill++;
                }
                finally {
                    this._syncing = undefined;
                }
            })();
        }
        return this._syncing;
    }
    flush(db) {
        const batch = this._batch;
        this._batch = [];
        return db.batch(batch, { valueEncoding: 'json' })
            .catch(err => {
            this._batch = batch.concat(this._batch);
            throw err;
        });
    }
    get db() {
        const db = lookup.get(this);
        if (db === undefined) {
            throw new Error('No db for log');
        }
        return db;
    }
}
exports.Partition = Partition;
function keyTimestamp(k) {
    return readTimestampBE(k, k.length - 12);
}
exports.maxCollisions = 8;
const MAX_UINT_32 = 0xffffffff;
const mask = 0x0100000000;
function writeTimestampBE(buf, ts, offset = 0) {
    buf.writeUInt32BE(~~(ts / mask), offset);
    buf.writeUInt32BE(ts % mask, offset + 4);
    return offset + 8;
}
function readTimestampBE(buf, offset = 0) {
    return (buf.readUInt32BE(offset) * mask) + buf.readUInt32BE(offset + 4);
}
