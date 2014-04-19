describe("LMDB environment", function()
    local os = require 'os'
    local lmdb = require 'lmdb'
    local utils = require 'utils'
    local dump = utils.dump
    local testdb = './db/test'
    local env, msg = nil

    before_each(function()
        env, msg = lmdb.env:open(testdb, {subdir = false, max_dbs=8})
    end)

    after_each(function()
        if env then
            env:close()
            env = nil
            msg = nil
        end
        collectgarbage()
        os.remove(testdb)
        os.remove(testdb .. '-lock')
    end)

    it("checks for environment clean open", function()
        assert.is_nil(msg)
        assert.not_nil(env)
        assert.not_nil(env['dbs'][0])
        assert.equals(testdb, env:path())
    end)

    it("check for read transaction", function()
        local txn, msg = lmdb.txn:begin(env, lmdb.READ_ONLY)
        assert.is_nil(msg)
        assert.not_nil(txn)
        assert.is_true(txn.read_only)
    end)

    it("check database open", function()
        local test_db, msg = env:db_open('calin')
        assert.is_nil(msg)
        assert.not_nil(test_db)
    end)

    it("check read after commited write", function()
        local t = os.time()
        env:transaction(function(txn)
            txn:put('test-key',t)
        end, lmdb.WRITE)
        local got_t = nil
        env:transaction(function(txn)
            got_t = txn:get('test-key')
        end, lmdb.READ_ONLY)
        assert.equals(t, tonumber(tostring(got_t)))
    end)
end)
