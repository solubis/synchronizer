/**
 * Created with JetBrains WebStorm.
 * User: yoorek
 * Date: 02.08.2013
 * Time: 17:42
 * To change this template use File | Settings | File Templates.
 */

"use strict";

if (typeof define !== 'function') {
    var define = require('amdefine')(module);
}

define(function (require) {

    var utils = require("./utils");
    var async = require("./async");

    var respond = utils.respond,
        extend = utils.extend,
        config = utils.config,
        guid = utils.guid,
        log = utils.log;

    var Database = extend(Object, {

        initialize: function () {
            var changeLog = true,
                changeLogTable = 'ChangeLog',
                primaryKey = 'id',
                logSQL = config.queryLogEnabled || false;

            this.isQueryLogEnabled = function () {
                return logSQL;
            };

            this.setQueryLog = function (flag) {
                logSQL = flag;
            };

            this.getPrimaryKey = function () {
                return primaryKey;
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
        },

        open: function (callback) {
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

        getTriggerSQL: function (table, action) {
            var operation = action.charAt(0);
            var id = (operation === 'D' ? "OLD.id" : "NEW.id");

            return "CREATE TRIGGER " + table + "_AFTER_" + action + " AFTER " + action + " ON " + table + " FOR EACH ROW " +
                "BEGIN " +
                "DELETE FROM ChangeLog WHERE object_id = " + id + ";"+
                "INSERT INTO ChangeLog (object_id, tablename, timestamp, operation) VALUES (" +
                id + ", '" +
                table + "', " +
                this.getSQLMapping("now()") + ", '" +
                operation + "'); " +
                "END;";
        },

        getSQLMapping: function (type, length) {
            return type;
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
                    return respond(callback, error);
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
            var me = this, count = 0;

            if (!(data instanceof Array)){
                return callback(new Error("Data is not array"));
            }

            async.forEach(data, function (row, onComplete) {
                var tablename = row.table;

                delete row.table;

                function isDeletedObject(object) {
                    return Object.keys(object).length === 1 && object[me.getPrimaryKey()];
                }

                if (isDeletedObject(row)) {
                    me.remove(row, tablename, onComplete);
                } else {
                    me.save(row, tablename, onComplete);
                }
                count++;

            }, function (error) {
                callback(error, count);
            });

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
                                try {
                                    delete row[key];
                                } catch (exception) {

                                }
                            }
                        });
                    });
                }

                if (error) {
                    return respond(callback, error);
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
                    return respond(callback, error);
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
                    return respond(callback, error);
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
                    return respond(callback, error);
                }
                if (!result.rowsAffected) {
                    log("WARNING! Insert with no rows affected");
                }
                return respond(callback, null, id);
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
                }
                respond(callback, null, result.rowsAffected);
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

                if (!result.rowsAffected) {
                    log("WARNING! Update with no rows affected");
                }

                respond(callback, null, id);
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
                    return respond(callback, error);
                }

                async.forEach(Object.keys(schema), function (table, complete) {
                    me.createTable(table, schema[table], complete);
                }, function finalize(error) {
                    return respond(callback, error);
                });
            });
        },

        createTable: function (name, model, callback) {
            var me = this,
                dropSQL = "DROP TABLE IF EXISTS " + name,
                createSQL = "CREATE TABLE " + name + "(",
                primaryKey = model.primaryKey || me.getPrimaryKey(),
                hasAutoincrement = false,
                columns = [];

            model.fields.forEach(function (columnDef) {
                var name = columnDef.name,
                    type = me.getSQLMapping(columnDef.type || "string", columnDef.length),
                    isRequired = columnDef.required || (columnDef.name === primaryKey),
                    column = [];

                if (columnDef.type === "autoincrement") {
                    hasAutoincrement = true;
                }

                column.push(name);
                column.push(type);
                column.push(isRequired ? "NOT NULL" : "NULL");

                columns.push(column.join(" "));
            });

            if (!hasAutoincrement) {
                columns.push("PRIMARY KEY(" + primaryKey + ")");
            }
            createSQL += columns.join(",");
            createSQL += ")";

            async.series([
                function (callback) {
                    me.executeSQL(dropSQL, callback);
                },
                function (callback) {
                    me.executeSQL(createSQL, callback);
                },
                function (callback) {
                    if (name !== 'ChangeLog') {
                        me.executeSQL(me.getTriggerSQL(name, 'INSERT'), callback);
                    } else {
                        callback();
                    }
                },
                function (callback) {
                    if (name !== 'ChangeLog') {
                        me.executeSQL(me.getTriggerSQL(name, 'DELETE'), callback);
                    } else {
                        callback();
                    }
                },
                function (callback) {
                    if (name !== 'ChangeLog') {
                        me.executeSQL(me.getTriggerSQL(name, 'UPDATE'), callback);
                    } else {
                        callback();
                    }
                }],

                function finalize(error) {
                    respond(callback, error);
                }
            );
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
        },

        getLogForObject: function (id, callback) {
            var sql = "SELECT * FROM " + this.getChangeLogTable();

            this.query(sql, callback);
        },

        getChangedData: function (start, callback) {
            var me = this,
                count,
                sql,
                data = [];

            function addRowToResult(row, table) {
                var result = {};

                count--;

                if (row) {
                    row.table = table;
                    data.push(row);
                }

                if (count === 0) {
                    result = {
                        data: data
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

            if (!start || !(start instanceof Date)) {
                return respond(callback, new Error("Pass start date"));
            }

            log("Changed data since: " + start);

            start = me.date(start);

            sql = "SELECT * FROM " + me.getChangeLogTable() + " WHERE timestamp > ? ORDER BY timestamp";

            me.query(sql, [start],
                function (error, rows) {
                    var result, log, length;

                    if (error) {
                        return respond(callback, error);
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
                            me.findById(log.tablename, log.object_id, addRowToResultFactory(log.tablename));
                        }
                    }
                });
        },

        getAllData: function (callback) {
            var me = this,
                count,
                data = [];

            function addRowsToResult(rows, table) {
                var result = {};

                if (rows.length > 0) {

                    for (var i = 0; i < rows.length; i++) {
                        rows[i].table = table;
                    }

                    data = data.concat(rows);
                }

                count--;

                if (count === 0) {
                    result = {
                        data: data
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

            me.getSchemaDefinition(function (error, schema) {
                if (error) {
                    return respond(callback, error, null);
                }

                count = Object.keys(schema).length - Database.noLog.length;

                Object.keys(schema).forEach(function (table) {
                    if (Database.noLog.indexOf(table) < 0) {
                        me.findAll(table, addRowsToResultFactory(table));
                    }
                });
            });
        }
    });

    Database.noLog = ["ChangeLog"];

    return Database;
});
