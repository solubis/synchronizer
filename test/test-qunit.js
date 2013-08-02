/**
 *    test-synch.js
 *
 *    Created by Jerzy Blaszczyk on 2011-11-03.
 *    Copyright 2011 Client and Friends. All rights reserved.
 */

/*global QUnit, solubis, console, asyncTest, equal, notEqual, start, ok*/

"use strict";

var step;

step = solubis.utils.step;

QUnit.testStart = function (test) {
    console.log("Test: " + test.module + " - " + test.name);
};

module("Synchronizer", {
    setup: function () {

        solubis.config.set("serverURL", "http://localhost:8888");
        solubis.config.set("database", "timtrak");
        solubis.config.set("clientUID", "Mac");

        this.db = new solubis.data.WebSQLiteDatabase(solubis.config.database);
        this.db.setQueryLog(false);
        this.sync = new solubis.data.SynchronizerClient(
            this.db,
            solubis.config.get("serverURL"),
            solubis.config.get("clientUID")
        );
    }
});

asyncTest("Create database", 1, function () {
    var me = this;

    step(
        function () {
            me.db.open(this);
        },
        function () {
            me.db.createTables(this);
        },
        function (error) {
            equal(error, null, "Funkcja powinna zwrócić liczbę obiektów");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.db.close();
            start();
        }
    );
});

asyncTest("Insert record", 1, function () {
    var me = this;

    step(
        function () {
            me.db.open(this);
        },
        function (error) {
            me.db.add({ name: "Task Name"}, 'Task', this);
        },
        function (error, id) {
            notEqual(id, null, "Funkcja powinna zwrócić rekord z wypełnionym kluczem głównym");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.db.close();
            start();
        }
    );
});

asyncTest("Delete record", 2, function () {
    var me = this;

    step(
        function () {
            me.db.open(this);
        },
        function (error) {
            me.db.add({name: "Task Name"}, 'Task', this);
        },
        function (error, id) {
            notEqual(id, null, "ID nadany");
            me.db.remove({id: id}, 'Task', this);
        },
        function (error, count) {
            equal(count, 1, "Skasowanie zakończone sukcesem");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.db.close();
            start();
        }
    );
});

asyncTest("Update record", 1, function () {
    var me = this;

    step(
        function () {
            me.db.open(this);
        },
        function (error) {
            me.db.add({name: 'Task Name' }, 'Task', this);
        },
        function (error, result) {
            me.db.save({id: result.id, name: 'Updated by update'}, 'Task', this);
        },
        function (error, result) {
            ok(true, "Update zakończony sukcesem");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.db.close();
            start();
        }
    );
});

asyncTest("Save record", 1, function () {
    var me = this;
    step(
        function () {
            me.db.open(this);
        },
        function (error) {
            me.db.add({ name: 'Task Name'}, 'Task', this);
        },
        function (error, result) {
            me.db.save({id: result.id, name: 'Updated by save'}, 'Task', this);
        },
        function (error, result) {
            me.db.save({ name: 'New'}, 'Task', this);
        },
        function (error, id) {
            notEqual(id, null, "Funkcja powinna zwrócić rekord z wypełnionym kluczem głównym");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.db.close();
            start();
        }
    );
});

asyncTest("Select record by ID", 1, function () {
    var me = this;
    step(
        function () {
            me.db.open(this);
        },
        function (error) {
            me.db.add({ name: 'Task Name'}, 'Task', this);
        },
        function (error, id) {
            me.db.findById('Task', id, this);
        },
        function (error, result) {
            notEqual(result.id, null, "Liczba rekordów powinna być 1");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.db.close();
            start();
        }
    );
});

asyncTest("Check Change Log", 5, function () {
    var me = this,
        id,
        o = {
            name: 'Change Log Test'
        };

    step(
        function () {
            me.sync.init(this);
        },
        function (error) {
            me.db.add(o, 'Task', this);
        },
        function (error, pid) {
            id = pid;
            me.sync.readLogForObject(id, this);
        },
        function (error, result) {
            equal(result.operation, 'I', "Wpis do logu o typie I");
            o = {
                id: id,
                name: 'Updated Change Log Test'
            };
            me.db.save(o, 'Task', this);
        },
        function (error, result) {
            equal(result, id, "Po zapisie istniejącego obiektu id powinien być taki sam");
            me.sync.readLogForObject(id, this);
        },
        function (error, result) {
            equal(result.count, 1, "Liczba wpisów do logu powinna być 1");
            equal(result.operation, 'I', "Wpis do logu o typie I");
            me.db.remove(o, 'Task', this);
        },
        function (error, result) {
            me.sync.readLogForObject(id, this);
        },
        function (error, result) {
            equal(result, null, "Liczba wpisów do logu powinna być 0");
            this();
        },
        function finalize(error) {
            if (error) {
                ok(false, error.message);
            }
            me.sync.done();
            start();
        }
    );
});

asyncTest("Synchronize changes", 1, function () {
    var me = this;
    step(
        function () {
            me.sync.init(this);
        },
        function () {
            me.sync.getChangedData(this);
        },
        function (error, clientdata) {
            me.sync.exchangeData(clientdata, this);
        },
        function (error, serverdata) {
            me.sync.enableChangeLog(false);
            me.db.batchUpdate(serverdata, this);
        },
        function () {
            me.db.clearTable(me.sync.getChangeLogTable(), this);
        },
        function finalize(error) {
            me.sync.enableChangeLog(true);
            equal(error, null, error ? error.message : "brak błędu");
            me.sync.done();
            start();
        }
    );
});

asyncTest("Get All Data", 1, function () {
    var me = this;
    step(
        function () {
            me.sync.init(this);
        },
        function () {
            me.sync.requestAllData(this);
        },
        function (error, serverdata) {
            me.db.batchUpdate(serverdata, this);
        },
        function finalize(error) {
            equal(error, null, error ? error.message : "brak błędu");
            me.sync.done();
            start();
        }
    );
});
