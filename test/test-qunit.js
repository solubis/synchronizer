/**
 *    test-synch.js
 *
 *    Created by Jerzy Blaszczyk on 2011-11-03.
 *    Copyright 2011 Client and Friends. All rights reserved.
 */

/*global QUnit, console, asyncTest, equal, notEqual, start, ok*/

"use strict";

require(["../shared/async", "../client/websqlite", "../client/remote", "../shared/utils"], function (async, sqlite, remote, utils) {

    QUnit.testStart = function (test) {
        console.log("Test: " + test.module + " - " + test.name);
    };

    module("Synchronizer", {
        setup: function () {

            this.db = new sqlite("test");
            this.db.setQueryLog(false);

            this.remote = new remote("http://localhost:3000", "test", "mac");
        }
    });

    asyncTest("Create database", function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.createTables(callback);
            }],
            function finalize(error) {
                ok(error === null);
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Insert record", function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add({ name: "Task Name"}, 'Task', callback);
            },
            function (result, callback) {
                notEqual(result, null, "Funkcja powinna zwrócić rekord z wypełnionym kluczem głównym");
                callback();
            }],
            function finalize(error, result) {
                ok(!error);

                me.db.close();
                start();
            }
        );
    });

    asyncTest("Delete record", function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add({name: "Task Name"}, 'Task', callback);
            },
            function (result, callback) {
                notEqual(result, null, "Add should return new ID of record");
                me.db.remove({id: result}, 'Task', callback);
            },
            function (result, callback) {

                callback();
            }],
            function finalize(error) {
                ok(!error);
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Update record", function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add({name: 'Task Name' }, 'Task', callback);
            },
            function (result, callback) {
                me.db.save({id: result.id, name: 'Updated by update'}, 'Task', callback);
            }],
            function finalize(error) {
                ok(!error);
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Save record", function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add({ name: 'Task Name'}, 'Task', callback);
            },
            function (result, callback) {
                me.db.save({id: result.id, name: 'Updated by save'}, 'Task', callback);
            },
            function (result, callback) {
                me.db.save({ name: 'New'}, 'Task', callback);
            },
            function (result, callback) {
                notEqual(result, null, "Funkcja powinna zwrócić rekord z wypełnionym kluczem głównym");
                callback();
            }],
            function finalize(error) {
                ok(!error);
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Select record by ID", function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add({ name: 'Task Name'}, 'Task', callback);
            },
            function (result, callback) {
                me.db.findById('Task', result, callback);
            },
            function (result, callback) {
                notEqual(result.id, null, "Liczba rekordów powinna być 1");
                callback();
            }],
            function finalize(error) {
                ok(!error);
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Check Change Log", function () {
        var me = this,
            id,
            o = {
                name: 'Change Log Test'
            };

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add(o, 'Task', callback);
            },
            function (result, callback) {
                id = result;
                me.db.getLogForObject(id, callback);
            },
            function (result, callback) {
                equal(result.length, 1, "ChangeLog count should be 1");
                equal(result[0].operation, 'I', "Log operation should be I");
                o = {
                    id  : id,
                    name: 'Updated Change Log Test'
                };
                me.db.save(o, 'Task', callback);
            },
            function (result, callback) {
                equal(result, id, "Po zapisie istniejącego obiektu id powinien być taki sam");
                me.db.getLogForObject(id, callback);
            },
            function (result, callback) {
                equal(result.length, 1, "Liczba wpisów do logu powinna być 1");
                equal(result[0].operation, 'U', "Wpis do logu o typie U");
                me.db.remove(o, 'Task', callback);
            },
            function (result, callback) {
                me.db.getLogForObject(id, callback);
            },
            function (result, callback) {
                equal(result.length, 1, "Liczba wpisów do logu powinna być 1");
                equal(result[0].operation, 'D', "Wpis do logu o typie D");
                callback();
            }],
            function finalize(error) {
                if (error) {
                    ok(false, error.message);
                }
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Synchronize changes", 1, function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.db.add({name:'Test'}, 'Task', callback);
            },
            function (result, callback) {
                me.db.getChangedData(new Date(1900,1,1), callback);
            },
            function (result, callback) {
                me.remote.sync(result.data, callback);
            },
            function (result, callback) {
                me.db.batchUpdate(result.data, callback);
            },
            function (result, callback) {
                me.db.clearTable(me.db.getChangeLogTable(), callback);
            }],
            function finalize(error) {
                equal(error, null, error ? error.message : "brak błędu");
                me.db.close();
                start();
            }
        );
    });

    asyncTest("Get All Data", 1, function () {
        var me = this;

        async.waterfall([
            function (callback) {
                me.db.open(callback);
            },
            function (callback) {
                me.remote.sync(callback);
            },
            function (result, callback) {
                me.db.batchUpdate(result.data, callback);
            }],
            function finalize(error) {
                equal(error, null, error ? error.message : "brak błędu");
                me.db.close();
                start();
            }
        );
    });
});
