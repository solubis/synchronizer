/**
 *  utils.js
 *
 *  Utilities module used for synchronizing local data with server
 *
 *  Created by Jerzy Blaszczyk on 2011-10-31.
 *  Copyright 2011 Solubis . All rights reserved.
 */

"use strict";

var async = require("async");
var request = require("request");

(function () {

    var utils = {};
    utils.config = {};

    utils.respond = function (callback, error, result) {
        if (callback !== undefined && "function" !== typeof callback) {
            throw new Error("Callback is not a function");
        }
        if (error) {
            console.error("[Error]", error.message);
            result = null;
        }
        if (callback !== undefined) {
            return callback(error, result);
        }
    };

    utils.ajax = function ajax(url, command, data, callback) {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', url + '/' + command, true);
        xhr.setRequestHeader("Content-Type", "application/json");
        xhr.onreadystatechange = function () {
            if (xhr.readyState === 4) {
                if (xhr.status === 0) {
                    return utils.respond(callback, new Error("Server is not responding"), null);
                }
                if (xhr.status === 200) {
                    var result = xhr.responseText;

                    try {
                        result = JSON.parse(xhr.responseText);
                        if (result.error) {
                            return utils.respond(callback, new Error(result.error.message), null);
                        }
                    }
                    catch (e) {
                        // if JSON.parse ends with error we pass back raw result
                    }
                    return utils.respond(callback, null, result);
                } else {
                    return utils.respond(callback, new Error(xhr.statusText + " : " + xhr.responseText), null);
                }
            }
        };

        if (typeof(data) !== "string") {
            data = JSON.stringify(data);
        }

        xhr.send(data);
    };

    utils.guid = function () {

        function getRandomString() {
            return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
        }

        return (getRandomString() + getRandomString() + "-" + getRandomString() + "-" + getRandomString() + "-" + getRandomString() + "-" + getRandomString() + getRandomString() + getRandomString()).toUpperCase();
    };

    utils.log = function () {
        console.log.apply(console, arguments);
    };

    utils.config.get = function (key) {
        var value = utils.config[key];

        if (!value) {
            console.log("Value for key:" + key + " not set in config.");
        }

        return value;
    };

    utils.config.set = function (key, value) {
        utils.config[key] = value;
        return value;
    };

    utils.extend = function (Parent, def) {
        var __extending = "__EXTENDING";

        if (!Parent) {
            throw new Error("Extending undefined parent.");
        }

        if (!def) {
            def = {};
        }

        var func = function () {
            if (arguments[0] === __extending) {
                return;
            }

            this.initialize.apply(this, arguments);
        };

        if ('function' === typeof(Parent)) {
            func.prototype = new Parent(__extending);
            func.prototype.superclass = Parent.prototype;
        }

        function copyMembers(obj, def) {

            Object.keys(def).forEach(function (member) {
                obj[member] = def[member];
            });

            obj.initEvents = function () {
                this.listeners = {};
            };

            obj.on = function (event, handler) {
                this.listeners[event] = this.listeners[event] || [];
                this.listeners[event].push(handler);
            };

            obj.fireEvent = function (event, param, onComplete) {
                if (this.listeners[event]) {
                    async.forEach(this.listeners[event], function (eventhandler, callback) {
                        eventhandler.call(this, param, callback);
                    }, function (error) {
                        return onComplete(error);
                    });
                } else {
                    onComplete();
                }
            };
        }

        copyMembers(func.prototype, def);

        return func;
    };

    if (typeof exports !== "undefined") {
        var util = require("util");

        utils.log = function () {
            var text = util.format.apply(this, arguments);
            util.log(text);
        };

        module.exports = utils;
    }
})();

