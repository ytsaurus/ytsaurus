var url = require("url");

var Q = require("bluebird");
var _ = require("underscore");
var utils = require("./utils");

var YtError = require("./error").that;
var YtHttpRequest = require("./http_request").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("V", "Versions");

////////////////////////////////////////////////////////////////////////////////

function YtApplicationVersions(driver)
{
    function getDataFromOrchid(entity, name)
    {
        return driver
        .executeSimple("get", { path: "//sys/" + entity + "/" + name + "/orchid/service"})
        .then(function(result) {
            return utils.pick(result, ["start_time", "version"]);
        });
    }

    function getListAndData(entity, dataLoader, nameExtractor)
    {
        nameExtractor = nameExtractor || _.keys;

        return driver.executeSimple(
            "get",
            { path: "//sys/" + entity })
        .then(function(names) {
            __DBG("Got " + entity + ": " + names);

            names = nameExtractor(names);

            return Q.settle(names.map(function(name) {
                return dataLoader(entity, name);
            }))
            .then(function(responses) {
                var result = {};
                for (var i = 0, len = responses.length; i < len; ++i) {
                    result[names[i]] = responses[i].isFulfilled() ? responses[i].value() : null;
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
            "primary_masters": getListAndData("primary_masters", getDataFromOrchid),
            "secondary_masters": getListAndData("secondary_masters", getDataFromOrchid, function (data) {
                return _.flatten(_.map(data, function (value, cell_name) {
                    return _.map(value, function (value, name) {
                        return cell_name + "/" + name;
                    })
                })); 
            }),
            "nodes": getListAndData("nodes", getDataFromOrchid),
            "schedulers": getListAndData("scheduler/instances", getDataFromOrchid),
            "proxies": getListAndData("proxies", function(entity, name) {
                return new YtHttpRequest(name)
                .withPath(url.format({
                    pathname: "/version"
                }))
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
