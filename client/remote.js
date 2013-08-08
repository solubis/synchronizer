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
    var async = require("../shared/async");

    var respond = utils.respond,
        extend = utils.extend,
        ajax = utils.ajax;

    var Client = extend(Object, {

        initialize: function (serverURL, database, clientUID) {
            this.serverURL = serverURL;
            this.clientUID = clientUID;
            this.database = database;
        },

        sync: function (data, callback) {
            var me = this;

            if (typeof data === 'function') {
                callback = data;
                data = {};
            }

            data = data || {};
            data.clientUID = me.clientUID;
            data.database = me.database;

            function complete(error, result) {
                if (error) {
                    return respond(callback, error, null);
                }
                respond(callback, null, result);
            }

            ajax(me.serverURL, 'sync', data, complete);
        }
    });

    return Client;
});