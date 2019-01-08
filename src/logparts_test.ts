import { LevelUp } from "levelup";
import os from "os"
import { Log } from "./logparts";
import assert from "assert";
import rimraf from "rimraf";

describe('logparts.DB', () => {
    const dir = os.tmpdir()
    let db :LevelUp
    interface P {
        foo: string,
        bar: string,
    }
    interface T {
        name: string,
        age: number,
    }

    before(() => {
        db = require("level")(dir)
    })
    after(() => {
        db.close()
        rimraf.sync(dir)
    })
    it("Opens a log for a partition", async () => {
        const logs = new Log<P, T>(db, ["foo", "bar"])
        const log = await logs.partition({
            foo: "FOO",
            bar: "BAR",
        })
        assert(log !== undefined, 'Log not found')
        const p = await db.get(logs.partitionKey(log!.id), {
            valueEncoding: 'json'
        })
        assert.deepEqual(p, {
            foo: "FOO",
            bar: "BAR",
        })
        const now = new Date()
        let item = await log!.shift(now)
        assert(item == undefined, 'item is empty')
        log!.push({
            name: "John",
            age: 23,
        }, now)
        item = await log!.shift(now)
        assert(item !== undefined, 'Item is undefined')
        assert.deepEqual(item, {
            name: "John",
            age: 23,
        })
        log!.push({
            name: "John",
            age: 23,
        }, new Date(now.getTime() + 10000))
        item = await log!.shift(now)
        assert(item == undefined, 'item is empty')


    })

})