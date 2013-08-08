"use strict";

var mysql = require("mysql");
var async = require("async");
var dateFormat = require("dateformat");
var fs = require("fs");
var utils = require("../shared/utils.js");

var Database = require("../shared/database.js");

var log = utils.log,
    respond = utils.respond,
    extend = utils.extend,
    dataFolder = __dirname + "/../data/";

var MySQLDatabase = (function () {

    var MySQLDatabase = extend(Database, {

            initialize: function (database, user, password) {
                this.user = user;
                this.password = password;
                this.database = database;

                this.superclass.initialize.call(this);
            },

            getSchemaDefinition: function (callback) {
                var me = this, schema = null;

                async.waterfall([
                    function (callback) {
                        fs.readFile(dataFolder + me.database + "/schema.json", "utf8", callback);
                    }],
                    function finalize(error, result) {
                        if (error) {
                            return respond(callback, error, null);
                        }

                        schema = JSON.parse(result);

                        respond(callback, null, schema);
                    });
            },

            open: function (callback) {
                var me = this;

                me.client = mysql.createConnection({
                    user    : this.user,
                    password: this.password
                });

                me.client.query('USE ' + this.database, function (error) {
                        if (error) {
                            if (error.errno === "ECONNREFUSED") {
                                return respond(callback, new Error("Connection to MySQL server refused"), null);
                            }
                            switch (error.number) {
                                case mysql.ERROR_BAD_DB_ERROR :
                                    me.client.query('CREATE DATABASE ' + me.database, function (error) {
                                            if (error) {
                                                me.opened = false;
                                                return callback(error, null);
                                            }
                                            me.opened = true;
                                            console.log("Created database " + me.database);
                                        }
                                    );
                                    break;
                                default:
                                    return respond(callback, new Error(error.message), null);
                            }
                        }
                        else {
                            me.opened = true;
                            respond(callback, null, null);
                        }
                    }
                );
            },

            executeSQL: function (sql, params, callback) {
                var me = this;

                if (!Array.isArray(params)) {
                    callback = params;
                    params = [];
                }

                if (!me.opened) {
                    return respond(callback, new Error("Database connection not opened"));
                }

                if (me.isQueryLogEnabled()) {
                    log('[SQL]', me.client.format(sql, params));
                }

                me.client.query(
                    sql, params,
                    function (error, data) {
                        var result = {};

                        if (error) {
                            console.error("[SQL Error]: ", error.message, error.sql);
                            return callback(error, null);
                        }

                        if (Array.isArray(data)) {
                            result.rows = data;
                        } else {
                            result.rowsAffected = data.affectedRows;
                        }

                        if (typeof callback === "function") {
                            callback(null, result);
                        }
                    }
                );
            },

            getSQLMapping: function (type, length) {
                switch (type) {
                    case "string":
                        if (length) {
                            return "varchar(" + length + ")";
                        } else {
                            return "varchar(40)";
                        }
                        break;
                    case "date":
                        return "datetime";
                    case "autoincrement":
                        return "int PRIMARY KEY AUTO_INCREMENT";

                    default:
                        return type;
                }
            },

            close: function () {
                if (this.client) {
                    this.client.end();
                }
            },

            date: function (date) {
                var s = dateFormat(date, 'yyyy-mm-dd HH:MM:ss');
                return s;
            }
        })
        ;

    return MySQLDatabase;

})();

if (typeof module !== 'undefined' && "exports" in module) {
    module.exports = MySQLDatabase;
}