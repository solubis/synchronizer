/**
 *  synchronizer.js
 *
 *  Synchronizer module used for synchronizing local data with server
 *
 *  Created by Jerzy Blaszczyk on 2011-10-31.
 *  Copyright 2011 Solubis . All rights reserved.
 */

/*global solubis, console, openDatabase, step*/

"use strict";

solubis.data.WebSQLiteDatabase = function () {

    var respond = solubis.utils.respond,
        counterCallback = solubis.utils.counterCallback,
        guid = solubis.utils.guid,
        extend = solubis.utils.extend,
        WebSQLiteDatabase;

    WebSQLiteDatabase = extend(solubis.data.SQLDatabase, {

        initialize: function (dbname) {
            this.database = dbname;

            this.dbConfig = {
                database: dbname,
                version: '1.00',
                description: 'SQLite Database',
                size: 5 * 1024 * 1024
            };

            this.superclass.initialize.call(this);
        },

        open: function (callback) {
            this.db = this.db || openDatabase(this.dbConfig.database, this.dbConfig.version, this.dbConfig.description, this.dbConfig.size);
            callback();
        },

        getSchemaDefinition: function (callback) {
            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }

                return respond(callback, null, result);
            }

            solubis.utils.ajax(solubis.config.serverURL, 'schema', {database: this.dbConfig.database, clientUID: solubis.config.clientUID}, complete);
        },

        executeSQL: function (sql, params, callback) {
            var me = this;

            if (!Array.isArray(params)) {
                callback = params;
                params = [];
            }

            if (me.isQueryLogEnabled()) {
                console.log('[SQL]', sql, params);
            }

            function onSuccess(tx, rs) {
                var rows = [], result, i;

                for (i = 0; i < rs.rows.length; i++) {
                    rows.push(rs.rows.item(i));
                }

                result = {rows: rows, rowsAffected: rs.rowsAffected};

                if (typeof callback === "function") {
                    callback(null, result);
                }
            }

            if (me.transaction) {
                me.transaction.executeSql(sql, params, onSuccess, me.onSQLError(sql, params, callback));
            } else {
                me.db.transaction(function (tx) {
                    tx.executeSql(sql, params, onSuccess, me.onSQLError(sql, params, callback));
                });
            }
        },

        createTable: function (name, model, callback) {
            var me = this,
                dropSQL = "DROP TABLE IF EXISTS " + name,
                createSQL = "CREATE TABLE " + name + "(",
                primaryKey = model.primaryKey || me.getPrimaryKey(),
                columns = [];

            model.fields.forEach(function (columnDef) {
                var name = columnDef.name,
                    type = me.getSQLType(columnDef.type || "string", columnDef.length),
                    isRequired = columnDef.required || (columnDef.name === primaryKey),
                    column = [];

                column.push(name);
                column.push(type);
                column.push(isRequired ? "NOT NULL" : "NULL");

                columns.push(column.join(" "));
            });

            columns.push("PRIMARY KEY(" + primaryKey + ")");
            createSQL += columns.join(",");
            createSQL += ")";

            step(
                function () {
                    me.executeSQL(dropSQL, this);
                },
                function (error) {
                    me.executeSQL(createSQL, callback);
                }
            );
        },

        getSQLType: function (type, length) {
            switch (type) {
                case "string":
                    return "text";
                case "date":
                    return "integer";
                default:
                    return type;
            }
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

            me.db.transaction(function (tx) {

                me.transaction = tx;

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

                me.transaction = null;
            });
        },

        close: function () {
        },

        date: function (date) {
            if (!date) {
                date = new Date();
            }
            return date.getTime();
        },

        onSQLError: function (sql, values, callback) {
            var me = this;

            return function (tx, err) {
                me.throwSQLError(err, sql, values);
                callback(err, null);
            };
        },

        throwSQLError: function (err, sql, params) {
            var error = new Error(err.message + ' in SQL: ' + sql + ' [' + (params || []) + ']');
            error.name = 'SQL Error';
            throw error;
        }
    });

    return WebSQLiteDatabase;
}();

