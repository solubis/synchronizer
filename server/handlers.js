"use strict";

var querystring = require("querystring"),
    fs = require("fs"),
    util = require("util"),
    MySQLDatabase = require("./database.mysql"),
    utils = require("../shared/utils"),
    step = utils.step,
    log = utils.log,
    Synchronizer = require("../shared/synchronizer");

var dataFolder = __dirname + "/../../data/";

var route = {
    "/test":test,
    "/data":data,
    "/sync":sync,
    "/schema":schema
};

utils.config.queryLogEnabled = true;

var db, sync, client, database;

function getJSONData(text) {
    var json;

    if (text) {
        try {
        json = JSON.parse(text);

        client = json.clientUID;
        database = json.database;

        log("Received data from '%s' for database '%s': \n\n%s\n", client, database, util.inspect(json, true, 4, true));
        } catch (e) {
            console.error("[ERROR]",e.message, text);
        }
    }
    return json;
}

function sendJSONData(data, response) {
    log("Sent to '%s' from database '%s': \n\n%s\n", client, database, util.inspect(data, true, 4, true));

    var result = JSON.stringify(data);
    setHeader(response, 200);
    response.write(result, "utf8");
}

function test(response, data) {
    fs.readFile(dataFolder + "data.json", function (error, file) {
        if (error) {
            handleError(error, 500, response);
        } else {
            setHeader(response, 200);
            response.write(file, "utf8");
            response.end();
        }
    });
}

function sync(response, data) {

    data = getJSONData(data);

    if (!data.clientUID || !data.database) {
        return handleError(new Error("Please provide clientUID and database as a parameter"), 500, response);
    }

    db = new MySQLDatabase(data.database, 'root', 'root');
    sync = new Synchronizer(db, data.clientUID);

    step(
        function () {
            sync.init(this);
        },
        function (error) {
            if (data)
                db.batchUpdate(data, this);
            else
                this();
        },
        function (error, total) {
            sync.getChangedData(this);
        },
        function (error, changes) {
            sendJSONData(changes, response);
            this();
        },
        function (error) {
            if (error)
                handleError(error, 500, response);
            else
                response.end();

            sync.commitCurrentSynchronization(); // Temporary AUTO-COMMIT TODO - move to client
        });
}

function data(response, data) {

    data = getJSONData(data);

    if (!data.clientUID || !data.database) {
        return handleError(new Error("Please provide clientUID and database as a parameter"), 500, response);
    }

    db = new MySQLDatabase(data.database, 'root', 'root');
    sync = new Synchronizer(db, data.clientUID);

    step(
        function () {
            sync.init(this);
        },
        function (error, total) {
            sync.getAllData(this);
        },
        function (error, tables) {
            sendJSONData(tables, response);
            this();
        },
        function finalize(error) {
            if (error)
                handleError(error, 500, response);
            else
                response.end();

            sync.commitCurrentSynchronization(); // Temporary AUTO-COMMIT TODO - move to client
        });
}

function schema(response, data) {

    data = getJSONData(data);

    if (!data || !data.clientUID || !data.database) {
        return handleError(new Error("Please provide clientUID and database as a parameter"), 500, response);
    }

    fs.readFile(dataFolder + data.database + "/schema.json", function (error, file) {
        if (error) {
            handleError(error, 500, response);
        } else {
            setHeader(response, 200);
            response.write(file, "text");
            response.end();
        }
    });
}

function handleError(error, status, response) {
    setHeader(response, status);
    response.write(error.message + "\n");
    response.end();
    console.error("[HTTP]", error.message);
}

function setHeader(response, status) {
    response.writeHead(status, {
        "Content-Type":"application/json",
        "Access-Control-Allow-Origin":"*",
        "Access-Control-Allow-Methods":"GET,POST",
        "Access-Control-Allow-Headers":"x-prototype-version,x-requested-with,content-type"
    });
}

exports.route = route;
