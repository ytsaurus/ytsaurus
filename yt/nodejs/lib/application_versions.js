var url = require("url");
var util = require("util");

var Q = require("bluebird");
var _ = require("underscore");
var utils = require("./utils");

var YtError = require("./error").that;
var YtHttpRequest = require("./http_request").that;
var YtRegistry = require("./registry").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("V", "Versions");

////////////////////////////////////////////////////////////////////////////////

var TIMEOUT = 10000;

function YtApplicationVersions(driver)
{
    var logger = YtRegistry.get("logger");

    function executeWithTimeout(commandName, parameters)
    {
        parameters.timeout = TIMEOUT;
        return driver.executeSimple(commandName, parameters);
    }

    function getDataFromOrchid(entity, name)
    {
        return executeWithTimeout("get", { path: "//sys/" + entity + "/" + name + "/orchid/service"})
        .then(function(result) {
            return utils.pick(utils.getYsonValue(result), ["start_time", "version"]);
        });
    }

    function getListAndData(entity, attributeList, dataLoader)
    {
        return executeWithTimeout("list", {
            path: "//sys/" + entity,
            attributes: attributeList
        })
        .then(function(names) {
            __DBG("Got " + entity + ": " + names);

            return Q.settle(_.map(names, function(name) {
                return dataLoader(entity, name);
            }))
            .then(function(responses) {
                var result = {};
                for (var i = 0, len = responses.length; i < len; ++i) {
                    result[utils.getYsonValue(names[i])] = responses[i].isFulfilled()
                        ? responses[i].value()
                        : {error: YtError.ensureWrapped(responses[i].error())};
                }
                return result;
            });
        })
        .catch(function(err) {
            return Q.reject(new YtError(
                "Failed to get list of " + entity + " names",
                err));
        });
    }

    function getListAndDataFromOrchid(entity, nameExtractor)
    {
        nameExtractor = nameExtractor || _.keys;

        return executeWithTimeout("get", { path: "//sys/" + entity })
        .then(function(names) {
            __DBG("Got " + entity + ": " + names);

            names = nameExtractor(names);

            var requests = names.map(function(name) {
                return {
                    command: "get",
                    parameters: { path: "//sys/" + entity + "/" + name + "/orchid/service"}
                }
            });

            return executeWithTimeout("execute_batch", {requests: requests})
            .then(function(values) {
                var result = {};
                for (var i = 0, len = values.length; i < len; ++i) {
                    if (values[i].error) {
                        result[names[i]] = {error: values[i].error};
                    } else {
                        result[names[i]] = utils.pick(utils.getYsonValue(values[i].output), ["start_time", "version"]);
                    }
                }
                return result;
            });
        })
        .catch(function(err) {
            return Q.reject(new YtError(
                "Failed to get list of " + entity + " names",
                err));
        });
    }

    this.get_versions = function() {
        return Q.props({
            "primary_masters": getListAndDataFromOrchid("primary_masters"),
            "secondary_masters": getListAndDataFromOrchid("secondary_masters", function (data) {
                return _.flatten(_.map(data, function (value, cell_name) {
                    return _.map(value, function (value, name) {
                        return cell_name + "/" + name;
                    })
                }));
            }),
            "nodes": getListAndData("nodes", ["addresses"], function(entity, name) {
                var addresses = utils.getYsonAttribute(name, "addresses");
                if ((typeof addresses === "undefined") || !addresses.hasOwnProperty("monitoring_http")) {
                    return Q.reject(new YtError("Failed to discover monitoring port"));
                }
                var parsed_url = url.parse("http://" + addresses["monitoring_http"]["default"]);
                return new YtHttpRequest(parsed_url.hostname, parsed_url.port)
                .withPath(url.format({
                    pathname: "/orchid/service"
                }))
                .setTimeout(TIMEOUT)
                .setNoResolve(true)
                .fire()
                .then(function (data) {
                    return utils.pick(JSON.parse(data.toString()), ["start_time", "version"]);
                });
            }),
            "schedulers": getListAndDataFromOrchid("scheduler/instances"),
            "controller_agents": getListAndDataFromOrchid("controller_agents/instances"),
            "proxies": getListAndData("proxies", [], function(entity, name) {
                var parsed_url = url.parse("http://" + utils.getYsonValue(name));
                return new YtHttpRequest(parsed_url.hostname, parsed_url.port)
                .withPath(url.format({
                    pathname: "/service"
                }))
                .setTimeout(TIMEOUT)
                .setNoResolve(true)
                .fire()
                .then(function (data) {
                    return utils.pick(JSON.parse(data.toString()), ["start_time", "version"]);
                });
            }),
        });
    };
}

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationVersions;
