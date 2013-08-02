"use strict";

var async = require("async");
var utils = require("../shared/utils");
var Database = require("../server/database.mysql.js");

var db = new Database('test', 'root', null);

db.setQueryLog(true);
db.open();
db.getSchemaDefinition(function (error, schema) {

    if (error) {
        return utils.log("error");
    }

    async.forEach(Object.keys(schema), function (table, complete) {
        utils.log("step");
        db.createTable(table, schema[table], complete);
    }, function finalize (error){
        utils.log("finalize");
        if (error) {
            return utils.log("error");
        }
    });
});
