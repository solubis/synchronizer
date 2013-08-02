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
    dataFolder = __dirname + "/../../data/";

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
                        isAutoincrement = columnDef.autoincrement,
                        column = [];

                    column.push(name);
                    column.push(type);
                    column.push(isRequired ? "NOT NULL" : "NULL");
                    if (isAutoincrement) {
                        column.push("AUTO_INCREMENT");
                    }

                    columns.push(column.join(" "));
                });

                columns.push("PRIMARY KEY(" + primaryKey + ")");
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

                        return respond(callback, error);
                    }

                )
                ;
            },

            getTriggerSQL: function (table, action) {
                var operation = action.charAt(0);
                var id = (operation === 'D' ? "OLD.id" : "NEW.id");

                return "CREATE TRIGGER " + table + "_AFTER_" + action + " AFTER " + action + " ON " + table + " FOR EACH ROW " +
                    " INSERT INTO ChangeLog SET object_id = "+ id + ", tablename = '" + table + "',timestamp = NOW(),operation = '" + operation + "'";
            },

            getSQLType: function (type, length) {
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

})
    ();

if (typeof module !== 'undefined' && "exports" in module) {
    module.exports = MySQLDatabase;
}

function dateToMysql(val) {
    return val.getUTCFullYear() + '-' +
        fillZeros(val.getUTCMonth() + 1) + '-' +
        fillZeros(val.getUTCDate()) + ' ' +
        fillZeros(val.getUTCHours()) + ':' +
        fillZeros(val.getUTCMinutes()) + ':' +
        fillZeros(val.getUTCSeconds());

    function fillZeros(v) {
        return v < 10 ? '0' + v : v;
    }
}