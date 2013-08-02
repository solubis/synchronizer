/**
 *  synchronizer.js
 *
 *  Synchronizer module used for synchronizing local data with server
 *
 *  Created by Jerzy Blaszczyk on 2011-10-31.
 *  Copyright 2011 Solubis . All rights reserved.
 */

/*global solubis, console*/

"use strict";

solubis.data.SynchronizerClient = (function () {

    var step = solubis.utils.step,
        respond = solubis.utils.respond,
        log = solubis.utils.log,
        extend = solubis.utils.extend,
        ajax = solubis.utils.ajax,
        Client;

    Client = extend(solubis.data.Synchronizer, {
        initialize: function (database, serverURL, clientUID) {
            this.superclass.initialize.call(this, database);

            this.getServerURL = function () {
                return serverURL;
            };

            if (clientUID) {
                this.clientUID = clientUID;
            }
        },

        init: function (callback) {
            var me = this;

            step(
                function () {
                    me.getDB().open(this);
                },
                function (error, result) {
                    me.getDB().find("Configuration", "parameter = ?", ['database_uid'], this);
                },
                function (error, result) {
                    if (!me.clientUID && Array.isArray(result) && result.length > 0) {
                        me.clientUID = result[0].value;
                    }
                    this();
                },
                function finalize(error) {
                    return respond(callback, error, null);
                }
            );
        },

        logChange: function (table, id, operation, callback) {
            var me = this,
                db = me.getDB(),
                log = {
                    object_id: id,
                    tablename: table,
                    operation: operation,
                    timestamp: db.date(new Date())
                };

            if (solubis.data.Synchronizer.noLog.indexOf(table) >= 0 || !me.isChangeLogEnabled()) {
                return respond(callback, null, null);
            }

            function complete(error, logEntry) {

                if (error) {
                    return respond(callback, error, null);
                }

                if (!logEntry) {
                    return db.add(log, me.getChangeLogTable(), callback);
                }

                log.id = logEntry.id;

                if (logEntry.operation === 'I' && operation === 'U') {
                    log.operation = 'I';
                }

                if (logEntry.operation === 'I' && operation === 'D') {
                    db.remove(log, me.getChangeLogTable(), callback);
                } else {
                    db.save(log, me.getChangeLogTable(), callback);
                }
            }

            me.readLogForObject(id, complete);
        },

        readLogForObject: function (id, callback) {
            var me = this,
                db = me.getDB(),
                sql;

            function complete(error, rows) {
                var log = {count: 0};

                if (error) {
                    return respond(callback, error, null);
                }

                if (rows.length > 1) {
                    return respond(callback, new Error("More than one log entry for single row"), null);
                }

                if (rows.length > 0) {
                    log = {
                        operation: rows[0].operation,
                        id: rows[0].id,
                        count: rows.length
                    };
                } else {
                    log = null;
                }

                return respond(callback, null, log);
            }

            db.find(me.getChangeLogTable(), "object_id = ?", [id], complete);
        },

        exchangeData: function (data, callback) {
            var me = this;

            if (typeof data === 'function') {
                callback = data;
                data = {};
            }

            data = data || {};
            data.clientUID = me.clientUID;
            data.database = solubis.config.database;

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }
                respond(callback, null, result);
            }

            ajax(me.getServerURL(), 'sync', data, complete);
        },

        requestAllData: function (data, callback) {
            var me = this;

            if (typeof data === 'function') {
                callback = data;
                data = {};
            }

            data = data || {};
            data.clientUID = me.clientUID;
            data.database = solubis.config.database;

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }
                respond(callback, null, result);
            }

            ajax(me.getServerURL(), 'data', data, complete);
        }


    });

    return Client;

}());