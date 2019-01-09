/// <reference types="node" />
import { LevelUp } from "levelup";
import { AbstractBatch, AbstractIteratorOptions } from "abstract-leveldown";
import { ParsedUrlQuery } from "querystring";
declare type Keys<T> = ReadonlyArray<keyof T>;
declare type Option<T> = T | undefined;
interface countResult extends Stats {
    entries: number;
    partitions: number;
    min: Date;
    max: Date;
    pending: number;
}
export declare class Log<P, T = any> {
    private _partitions;
    private _db;
    private _loading;
    readonly keys: Keys<P>;
    readonly keyPrefix: string;
    constructor(db: LevelUp, keys: Keys<P>, keyPrefix?: string);
    batch(batch: Batch<T>, options?: any): Promise<void>;
    hashValues(p: P, nonce?: string): string;
    scan(options: IteratorOptions, scanner: Scanner<T>): Promise<void>;
    /**
     * Finds or creates a partition for the specified values.
     *
     * Tries `DB.maxCollisions` times to resolve hash collisions
     * @param values values for the partition
     */
    partition(values: P): Promise<Option<Partition<P, T>>>;
    private _addPartition;
    match(q: ParsedUrlQuery, p: P): boolean;
    equal(a: P, b: P): boolean;
    private _matchAll;
    summary(): Promise<{
        entries: number;
        partitions: number;
        min: Date;
        max: Date;
    }>;
    count(q: ParsedUrlQuery): Promise<countResult>;
    partitionKey(id: string): Buffer;
    readonly partitionKeySize: number;
    private readonly minPartitionKey;
    private readonly maxPartitionKey;
    entryKey(logId: string, ts: number, id?: number): Buffer;
    readonly entryKeySize: number;
    private readonly minEntryKey;
    private readonly maxEntryKey;
    private _count;
    private _partitionValues;
    /**
     * Load or create a partition in db
     *
     * @param id the partition id
     * @param values the partition's values
     */
    private _load;
    /**
     * Load partition from the db once per id
     */
    private _loadOnce;
}
export interface ScanResult<T> {
    key: Buffer;
    value: T;
}
export declare type Scanner<T> = (r: ScanResult<T>) => void;
export declare type IteratorOptions = AbstractIteratorOptions<Buffer>;
export declare const defaultKeyPrefix = "db";
export declare type Batch<T> = AbstractBatch<Buffer, T>[];
export interface Stats {
    hit: number;
    miss: number;
    fill: number;
    flush: number;
    batch: number;
}
export declare class Partition<P, T> {
    readonly id: string;
    private _cache;
    private _syncing?;
    private _maxT;
    private _next;
    private _batch;
    private _stats;
    readonly values: Readonly<P>;
    constructor(id: string, values: P);
    readonly nextId: number;
    readonly batch: ReadonlyArray<AbstractBatch<Buffer, T>>;
    readonly stats: Readonly<Stats>;
    readonly maxDate: Date;
    shift(now: Date): Promise<Option<T>>;
    push(value: T, ts: Date): void;
    close(): Promise<void>;
    private fill;
    sync(now: Date): Promise<void>;
    private _sync;
    private flush;
    readonly db: Log<P, T>;
}
export declare const maxCollisions = 8;
export {};
//# sourceMappingURL=logparts.d.ts.map