/**
 *  synchronizer.js
 *
 *  Synchronizer module used for synchronizing local data with server
 *
 *  Created by Jerzy Blaszczyk on 2011-10-31.
 *  Copyright 2011 Solubis . All rights reserved.
 */

"use strict";

if (typeof define !== 'function') {
    var define = require('amdefine')(module);
}

define(function (require) {

    var utils = require("../shared/utils");
    var Database = require("../shared/database");
    var async = require("../shared/async");


    var respond = utils.respond,
        guid = utils.guid,
        extend = utils.extend;

    var WebSQLiteDatabase = extend(Database, {

        initialize: function (dbname) {
            this.dbConfig = {
                database   : dbname,
                version    : '1.00',
                description: 'SQLite Database',
                size       : 5 * 1024 * 1024
            };

            this.superclass.initialize.call(this);
        },

        open: function (callback) {
            this.db = this.db || openDatabase(this.dbConfig.database, this.dbConfig.version, this.dbConfig.description, this.dbConfig.size);
            if ("function" === typeof callback){
                callback();
            }
        },

        getSchemaDefinition: function (callback) {
            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }

                return respond(callback, null, result);
            }

            utils.ajax("http://localhost:3000", 'schema', {database: this.dbConfig.database, clientUID: utils.config.clientUID}, complete);
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

            function onError (sql, values, callback) {
                return function (tx, err) {
                    var error = new Error(err.message + ' in SQL: ' + sql + ' [' + (values || []) + ']');
                    error.name = 'SQL Error';
                    callback(error);
                    throw error;
                };
            }

            if (me.transaction) {
                me.transaction.executeSql(sql, params, onSuccess, onError(sql, params, callback));
            } else {
                me.db.transaction(function (tx) {
                    tx.executeSql(sql, params, onSuccess, onError(sql, params, callback));
                });
            }
        },

        getSQLMapping: function (type, length) {
            switch (type) {
                case "string":
                    return "text";
                case "date":
                    return "integer";
                case "autoincrement":
                    return "integer primary key autoincrement";
                case "now()":
                    return "datetime('now')";
                default:
                    return type;
            }
        },

        batchUpdate: function (data, callback) {
            var me = this;

            me.db.transaction(function (tx) {

                me.transaction = tx;

                me.superclass.batchUpdate.call(me, data, function(error, result) {
                    me.transaction = null;

                    callback(error, result);
                });
            });
        },

        close: function () {
        },

        date: function (date) {
            if (!date) {
                date = new Date();
            }
            return date.getTime();
        }
    });

    return WebSQLiteDatabase;
});

