"use strict";

var express = require('express');
var http = require('http');
var path = require('path');
var async = require('async');

var utils = require('./shared/utils');
var Database = require('./server/mysql');
var Synchronizer = require('./shared/synchronizer');

var app = express();
var log = utils.log;

// all environments
app.set('port', process.env.PORT || 3000);
app.set('views', __dirname + '/test');
app.set('view engine', 'blade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(express.cookieParser('your secret here'));
app.use(express.session());
app.use(app.router);
app.use(express.static(__dirname));


// development only
if ('development' === app.get('env')) {
    app.use(express.errorHandler());
}

var db = new Database('test', 'root');
var sync = new Synchronizer(db);

app.get('/', function (req, res) {
    res.render('test', {title: 'QUnit test'});
});

app.get('/schema', function (req, res) {
    res.sendfile('data/test/schema.json');
});

app.post('/schema', function (req, res) {
    res.sendfile('data/test/schema.json');
});

app.post('/sync', function (req, res) {
    async.waterfall([
        function (callback) {
            db.open(callback);
        },
        function (result, callback) {
            sync.getAllData(callback);
        }
    ], function (error, result) {
        if (error) {
            return res.render('error', {message: error.message});
        }
        res.json(result);
    });
});

http.createServer(app).listen(app.get('port'), function () {
    log('Express server listening on port ' + app.get('port'));
});
