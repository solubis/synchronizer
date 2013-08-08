"use strict";

var Database = require("../server/mysql.js");
var Synchronizer = require("../shared/synchronizer");
var async = require("async");

var db = new Database('test', 'root', null);
var sync = new Synchronizer(db);

db.setQueryLog(true);

exports.testCreateDatabase = function (test) {

    async.series([
        function (callback) {
            db.open(callback);
        },
        function (callback) {
            db.createTables(callback);
        }],
        function finalize(error) {
            test.ifError(error);
            test.done();

            db.close();
        }
    );
};

exports.testDatabaseCRUD = function (test) {
    var id;

    async.waterfall([
        function (callback) {
            db.open(callback);
        },
        function (result, callback) {
            db.add({name: "Test1"}, 'Task', callback);
        },
        function (result, callback) {
            test.ok(result);

            id = result;
            db.update({id: id, name: 'Updated'}, 'Task', callback);
        },
        function (result, callback) {
            test.ok(result, "Record updated");

            db.findById('Task', result, callback);
        },
        function (result, callback) {
            test.equal(result.id, id, "Proper ID");
            test.equal(result.name, "Updated", "Proper name");

            db.remove({id: result.id}, 'Task', callback);
        }],
        function finalize(error, result) {
            test.ifError(error);
            test.ok(result, 1, "1 Record deleted");
            test.done();

            db.close();
        }
    );
};

exports.testSynchronize = function (test) {
    var o2, o1, lastSync = new Date(1900,1,1);

    async.waterfall([
        function (callback) {
            db.open(callback);
        },
        function (result, callback) {
            db.createTables(callback);
        },
        function (result, callback) {
            o1 = {id: '1', name: "Test1"};
            o2 = {id: '2', name: "Test2"};

            async.parallel([
                function (callback) {
                    db.add(o1, 'Task', callback);
                },
                function (callback) {
                    db.add(o2, 'Task', callback);
                }
            ], function (error) {
                test.ifError(error);

                callback();
            });
        },

        // First Synchronization - 2 rows
        function (callback) {
            sync.getChangedData(new Date(1900,1,1), callback);
        },
        function (result, callback) {
            test.notEqual(result, null, "Changed data read");
            test.equal(result.data.length, 2, "Number of rows should be correct");

            setTimeout(callback, 1000); // zapamiętywać czas ostatniej synchronizacji
        },
        function (callback) {
            async.parallel([
                function (callback) {
                    db.add({id: '3', name: "New after synch"}, 'Task', callback);
                },
                function (callback) {
                    o1.name = "Updated after synch";
                    db.save(o1, 'Task', callback);
                },
                function (callback) {
                    db.remove(o2, 'Task', callback);
                }
            ], function (error) {
                test.ifError(error);

                callback();
            });

        },

        // New Synchronization
        function (callback) {
            sync.getChangedData(lastSync, callback);
        }],
        function finalize(error, result) {
            test.ifError(error);
            test.notEqual(result, null, "Changed data read");
            test.ok(result.data);

            if (result.data) {
                test.equal(result.data.length, 3, "Number of rows should be correct");
            }

            test.done();

            db.close();
        }
    );
};

exports.testFillDatabase = function (test) {

    async.waterfall([
        function (callback) {
            db.open(callback);
        },
        function (result, callback) {
            var noOfRows = 10;

            async.until(
                function () {
                    return noOfRows < 0;
                },
                function (callback) {
                    noOfRows--;

                    db.add({
                        name       : "Test Name",
                        description: "Description",
                        start      : db.date(new Date(2011, 0, 1)),
                    }, 'Task', callback);
                },
                function finalize(error) {
                    test.ifError(error);

                    callback();
                }
            );
        }],
        function finalize(error) {
            test.ifError(error);
            test.done();

            db.close();
        }
    );
};

exports.testGetAllData = function (test) {

    async.waterfall([
        function (callback) {
            db.open(callback);
        },
        function (result, callback) {
            sync.getAllData(callback);
        }],
        function finalize(error, result) {
            test.ifError(error);
            test.ok(result.data);

            if (result.data) {
                test.ok(result.data.length >= 3, "Number of rows should be correct");
            }
            test.done();

            db.close();
        }
    );
};

exports.testBatchUpdate = function (test) {

    var data = [
        {table: 'Task', id: '3', name: 'To Delet'},
        {table: 'Task', id: '3'},
        {table: 'Task', id: '2', name: 'Batch update'},
        {table: 'Task', id: '1', name: 'Batch update'},
        {table: 'Task', id: '4', name: 'New batch update'}
    ];

    async.waterfall([
        function (callback) {
            db.open(callback);
        },
        function (result, callback) {
            db.batchUpdate(data, callback);
        }],
        function finalize(error, result) {
            test.ifError(error);
            test.ok(result === 5);
            test.done();

            db.close();
        }
    );
};

