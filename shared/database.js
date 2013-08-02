/**
 *  database.js
 *
 *  Database helper
 *
 *  Copyright 2011  All rights reserved.
 */

"use strict";

var utils = require("./utils");
var async = require("async");

var AbstractDatabase = (function () {

    var respond = utils.respond,
        extend = utils.extend,
        config = utils.config,
        counterCallback = utils.counterCallback;

    var AbstractDatabase = extend(Object, {

        initialize: function () {
            var primaryKey = 'id', logSQL = config.queryLogEnabled || false;

            this.initEvents();

            this.isQueryLogEnabled = function () {
                return logSQL;
            };

            this.setQueryLog = function (flag) {
                logSQL = flag;
            };

            this.getPrimaryKey = function () {
                return primaryKey;
            };

            this.setPrimaryKey = function (fieldName) {
                primaryKey = fieldName;
            };
        },

        save: function (object, table, callback) {
            var me = this,
                id;

            id = object[me.getPrimaryKey()];

            if (!id) {
                return me.add(object, table, callback);
            }

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }

                if (result) {
                    me.update(object, table, callback);
                } else {
                    me.add(object, table, callback);
                }
            }

            me.exist(id, table, complete);
        },

        batchUpdate: function (data, callback) {
            var me = this,
                count = 0,
                table,
                object,
                counter;

            if (!data || !data.tables) {
                return respond(callback, null, null);
            }

            function isDeletedObject(object) {
                return Object.keys(object).length === 1 && object[me.getPrimaryKey()];
            }

            function complete(error) {
                respond(callback, error, count);
            }

            Object.keys(data.tables).forEach(function (tablename) {
                count = count + data.tables[tablename].length;
            });

            if (count === 0) {
                return respond(callback, null, 0);
            }

            counter = counterCallback(count, complete);

            Object.keys(data.tables).forEach(function (tablename) {
                table = data.tables[tablename];

                for (var j = 0; j < table.length; j++) {
                    object = table[j];

                    if (isDeletedObject(object)) {
                        me.remove(object, tablename, counter);
                    } else {
                        me.save(object, tablename, counter);
                    }
                }
            });
        }
    });

    return AbstractDatabase;

})();

var Database = (function () {

    var respond = utils.respond,
        counterCallback = utils.counterCallback,
        guid = utils.guid,
        extend = utils.extend,
        log = utils.log;

    var Database = extend(AbstractDatabase, {

        open: function () {
            throw new Error("open not implemented");
        },

        close: function () {
            throw new Error("close not implemented");
        },

        executeSQL: function (sql, params, callback) {
            throw new Error("executeSQL not implemented");
        },

        getSchemaDefinition: function (callback) {
            throw new Error("getSchemaDefinition not implemented");
        },

        createTable: function (name, model, callback) {
            throw new Error("createTable not implemented");
        },

        query: function (sql, params, callback) {
            if (!Array.isArray(params)) {
                callback = params;
                params = [];
            }

            function complete(error, result) {

                if (result.rows && result.rows.length > 0) {
                    result.rows.forEach(function (row) {
                        Object.keys(row).forEach(function (key) {
                            if (row[key] === null) {
                                delete row[key];
                            }
                        });
                    });
                }

                if (error) {
                    return respond(callback, error, null);
                }
                return respond(callback, null, result.rows);
            }

            this.executeSQL(sql, params, complete);
        },

        find: function (table, where, params, callback) {
            this.query("SELECT * FROM " + table + " WHERE " + where, params, callback);
        },

        findAll: function (table, callback) {
            this.query("SELECT * FROM " + table, callback);
        },

        findById: function (table, id, callback) {
            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }

                if (result.length > 1) {
                    respond(callback, new Error(result.length + " records with same id: " + result[0].id), result);
                }
                return respond(callback, null, result[0]);
            }

            this.query("SELECT * FROM " + table + " WHERE id = ?", [id], complete);
        },

        exist: function (id, table, callback) {
            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }
                return respond(callback, null, result.rows.length > 0);
            }

            this.executeSQL("SELECT 1 FROM " + table + " WHERE id = ?", [id], complete);
        },

        add: function (object, table, callback) {
            var me = this,
                sql,
                placeholders = [],
                id = object[me.getPrimaryKey()],
                values = [],
                columns = [];

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }
                if (!result.rowsAffected) {
                    log("WARNING! Insert with no rows affected");
                } else {
                    me.fireEvent('afterAdd', {table: table, id: id}, function () {
                        respond(callback, null, id);
                    });
                }
            }

            id = id || guid();

            columns.push(me.getPrimaryKey());
            placeholders.push('?');
            values.push(id);

            for (var field in object) {
                if (field !== me.getPrimaryKey()) {
                    placeholders.push('?');
                    values.push(object[field]);
                    columns.push(field);
                }
            }

            sql = "INSERT INTO " + table + " (" + columns.join(',') + ") VALUES (" + placeholders.join(',') + ")";
            me.executeSQL(sql, values, complete);
        },

        remove: function (object, table, callback) {
            var me = this,
                id = object[me.getPrimaryKey()],
                sql;

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }
                if (!result.rowsAffected) {
                    log("WARNING! Delete with no rows affected");
                } else {
                    me.fireEvent('afterRemove', {table: table, id: id}, function () {
                        respond(callback, null, result.rowsAffected);
                    });
                }
            }

            sql = "DELETE FROM " + table + ' WHERE id = ?';
            me.executeSQL(sql, [id], complete);
        },

        update: function (object, table, callback) {
            var me = this,
                sql,
                id = object[me.getPrimaryKey()],
                values = [],
                sets = [];

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }

                if (result.rowsAffected) {
                    me.fireEvent('afterUpdate', {table: table, id: id}, function () {
                        respond(callback, null, id);
                    });
                }
                else {
                    log("WARNING! Update with no rows affected");
                }
            }

            for (var field in object) {
                if (field !== me.getPrimaryKey()) {
                    values.push(object[field]);
                    sets.push(field + " = ?");
                }
            }

            values.push(id);

            sql = "UPDATE " + table + ' SET ' + sets.join(',') + ' WHERE id = ?';
            me.executeSQL(sql, values, complete);
        },

        createTables: function (callback) {
            var me = this;

            me.getSchemaDefinition(function (error, schema) {

                if (error) {
                    return respond(callback, error, null);
                }

                async.forEach(Object.keys(schema), function (table, complete) {
                    me.createTable(table, schema[table], complete);
                }, function finalize (error){
                    return respond (callback, error);
                });
            });
        },

        date: function (date) {
            return date;
        },

        clearTable: function (table, callback) {
            var sql = 'DELETE FROM ' + table;

            this.executeSQL(sql, [], callback);
        },

        getTableSize: function (table, callback) {
            var me = this;

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }

                return respond(callback, null, result.rows[0]['COUNT(*)']);
            }

            me.executeSQL('SELECT COUNT(*) FROM ' + table, [], complete);
        }

    });

    return Database;

})();

if (typeof module !== 'undefined' && "exports" in module) {
    module.exports = Database;
}