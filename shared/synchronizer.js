/**
 *  synchronizer.js
 *
 *  Synchronizer module used for synchronizing local data with server
 *
 *  Created by Jerzy Blaszczyk on 2011-10-31.
 *  Copyright 2011 Solubis . All rights reserved.
 */

"use strict";

var fs = require("fs");
var utils = require("../shared/utils");

var Synchronizer = function () {

    var respond = utils.respond,
        extend = utils.extend,
        log = utils.log;

    var Synchronizer = extend(Object, {

        initialize: function (database) {
            var me = this,
                changeLog = true,
                changeLogTable = 'ChangeLog';

            if (database === undefined) {
                throw new Error("You must specify database reference as parameter.");
            }

            database.sync = this;

            this.getDB = function () {
                return database;
            };

            this.enableChangeLog = function (flag) {
                changeLog = flag;
            };

            this.getChangeLogTable = function () {
                return changeLogTable;
            };

            this.isChangeLogEnabled = function () {
                return changeLog;
            };

            this.getDB = function () {
                return database;
            };
        },

        getChangedData: function (start, callback) {
            var me = this,
                db = me.getDB(),
                count,
                sql,
                tables = {};

            function addRowToResult(row, table) {
                var result = {};

                count--;

                if (tables[table] === undefined) {
                    tables[table] = [];
                }

                if (row) {
                    tables[table].push(row);
                }

                if (count === 0) {
                    result = {
                        tables: tables
                    };

                    respond(callback, null, result);
                }
            }

            function addRowToResultFactory(table) {
                return function (error, result) {
                    if (error) {
                        return callback(error, null);
                    }
                    addRowToResult(result, table);
                };
            }

            if (!start) {
                start = new Date(1900, 1, 1);
            }

            start = db.date(start);

            sql = "SELECT * FROM " + me.getChangeLogTable() + " WHERE timestamp > ? ORDER BY timestamp";

            log("Changed data since: [%s]", start);

            db.query(sql, [start],
                function (error, rows) {
                    var result, log, length;

                    if (error) {
                        return respond(callback, error, null);
                    }

                    if (!rows || rows.length === 0) {
                        result = {error: null, tables: []};
                        return respond(callback, null, result);
                    }

                    length = rows.length;
                    count = length;

                    for (var i = 0; i < rows.length; i++) {
                        log = rows[i];

                        if (log.operation === 'D') {
                            addRowToResult({id: log.object_id}, log.tablename);
                        } else {
                            db.findById(log.tablename, log.object_id, addRowToResultFactory(log.tablename));
                        }
                    }
                });
        },

        getAllData: function (callback) {
            var me = this,
                db = me.getDB(),
                count,
                tables = {};

            function addRowsToResult(rows, table) {
                var result = {};

                count--;

                if (rows.length > 0) {
                    tables[table] = rows;
                }

                if (count === 0) {
                    result = {
                        tables: tables
                    };

                    respond(callback, null, result);
                }
            }

            function addRowsToResultFactory(table) {
                return function (error, rows) {
                    if (error) {
                        return callback(error, null);
                    }
                    addRowsToResult(rows, table);
                };
            }

            db.getSchemaDefinition(function (error, schema) {
                if (error) {
                    return respond(callback, error, null);
                }

                count = Object.keys(schema).length - Synchronizer.noLog.length;

                Object.keys(schema).forEach(function (table) {
                    if (Synchronizer.noLog.indexOf(table) < 0) {
                        db.findAll(table, addRowsToResultFactory(table));
                    }
                });
            });

        }
    });

    Synchronizer.noLog = ["ChangeLog", "Configuration", "SynchronizationLog"];

    return Synchronizer;

}();

if (typeof module !== 'undefined' && module.exports) {
    module.exports = Synchronizer;
}