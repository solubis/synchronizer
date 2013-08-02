"use strict";

var http = require("http"),
    url = require("url"),
    querystring = require("querystring"),
    handlers = require("./handlers"),
    log = require("../shared/utils").log;

function start() {
    function onRequest(request, response) {
        var params = url.parse(request.url, true),
            pathname = params.pathname,
            data = "";

        log("Request for %s received with method: %s", pathname, request.method);

        request.setEncoding("utf8");

        request.addListener("data", function (dataChunk) {
            data += dataChunk;
        });

        request.addListener("end", function () {
            if (request.method === 'OPTIONS') {
                response.writeHead(200, {
                    "Content-Type":"application/json",
                    "Access-Control-Allow-Origin":"*",
                    "Access-Control-Allow-Methods":"GET,POST",
                    "Access-Control-Allow-Headers":"x-prototype-version,x-requested-with,content-type"
                });
                response.write("OPTIONS accepted");
                response.end();
            } else {
                route(pathname, response, data);
            }
        });
    }

    http.createServer(onRequest).listen(8888);
    log("Server has started.");
}

function route(pathname, response, data, database, clientUID) {
    log("About to route a request for: %s ", pathname);

    if (typeof handlers.route[pathname] === 'function') {
        handlers.route[pathname](response, data);
    } else {
        console.log("No request handler found for " + pathname);
        response.writeHead(404, {"Content-Type":"text/html"});
        response.write("404 Not found");
        response.end();
    }
}

start();