describe("LMDB cursors", function()
    local os = require 'os'
    local lmdb = require 'lmdb'
    local utils = require 'utils'
    local dump = utils.dump
    local testdb = './db/test-10k'
    local env, msg = nil

    setup(function()
        env, msg = lmdb.environment(testdb, {subdir = false, max_dbs=8})
        env:transaction(function(txn)
            for i=1,10000 do
                txn:put(i,i)
            end
        end, lmdb.WRITE)
    end)

    teardown(function()
        env = nil
        msg = nil
        collectgarbage()
        os.remove(testdb)
        os.remove(testdb .. '-lock')
    end)

    it("checks cursor simple iteration", function()
        env:transaction(function(txn)
            local i, c = 0, txn:cursor()
            for k,v in c:iter() do
                assert.equals(k, tostring(v))
            end
        end, lmdb.READ_ONLY)
    end)
end)
