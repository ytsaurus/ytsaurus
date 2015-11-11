var url = require("url");
var util = require('util');

var Q = require("bluebird");

var utils = require("./utils");

var YtError = require("./error").that;
var YtHttpRequest = require("./http_request").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("V", "Versions");

////////////////////////////////////////////////////////////////////////////////

var TIMEOUT = 1000;

function YtApplicationVersions(driver)
{
    function executeWithTimout(commandName, parameters)
    {
        parameters.timeout = TIMEOUT;
        return driver.executeSimple(commandName, parameters);
    }
    
    function getDataFromOrchid(entity, name)
    {
        return executeWithTimout("get", { path: "//sys/" + entity + "/" + name + "/orchid/service"})
        .then(function(result) {
            return utils.pick(result, ["start_time", "version"]);
        });
    }

    function getListAndData(entity, dataLoader)
    {
        return executeWithTimout(
            "list",
            { path: "//sys/" + entity })
        .then(function(names) {
            __DBG("Got " + entity + ": " + names);
            return Q.settle(names.map(function(name) {
                return dataLoader(entity, name);
            }))
            .then(function(responses) {
                var result = {};
                for (var i = 0, len = responses.length; i < len; ++i) {
                    result[names[i]] = responses[i].isFulfilled()
                        ? responses[i].value()
                        : {error: responses[i].error()};
                }
                return result;
            })
        })
        .catch(function(err) {
            return Q.reject(new YtError(
                "Failed to get list of " + entity + " names",
                err));
        });
    }

    this.get_versions = function() {
        return Q.props({
            "masters": getListAndData("masters", getDataFromOrchid),
            "nodes": getListAndData("nodes", getDataFromOrchid),
            "schedulers": getListAndData("scheduler/instances", getDataFromOrchid),
            "proxies": getListAndData("proxies", function(entity, name) {
                var parsed_url = url.parse("http://" + name);
                return new YtHttpRequest(parsed_url.hostname, parsed_url.port)
                .withPath(url.format({
                    pathname: "/version"
                }))
                .setTimeout(TIMEOUT)
                .setNoResolve(true)
                .fire()
                .then(function (data) {
                    return { version: data.toString() };
                });
            }),
        });
    };
}

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationVersions;
