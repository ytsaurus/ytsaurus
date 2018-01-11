var url = require("url")
var utils = require("../utils");
var worker = require("../worker");

////////////////////////////////////////////////////////////////////////////////

function endsWith(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) !== -1;
}

exports.that = function Middleware__YtAcao()
{
    return function(req, rsp, next) {
        var origin = req.headers["origin"] || "*";

        var allow = false;
        if (origin !== "*") {
            var hostname = url.parse(origin).hostname;
            if (hostname === "localhost" || endsWith(hostname, ".yandex.net") || endsWith(hostname, ".yandex-team.ru")) {
                allow = true;
            }
        }

        if (allow) {
            if (req.method === "GET" || req.method === "POST" || req.method === "PUT") {
                rsp.setHeader("Access-Control-Allow-Credentials", "true");
                rsp.setHeader("Access-Control-Allow-Origin", origin);
            }

            if (req.method === "OPTIONS") {
                rsp.setHeader("Access-Control-Allow-Credentials", "true");
                rsp.setHeader("Access-Control-Allow-Origin", origin);
                rsp.setHeader("Access-Control-Allow-Methods", "POST, PUT, GET, OPTIONS");
                var cors_headers = [
                    "authorization",
                    "origin",
                    "content-type",
                    "accept",
                    "x-yt-parameters",
                    "x-yt-parameters0",
                    "x-yt-parameters-0",
                    "x-yt-parameters1",
                    "x-yt-parameters-1",
                    "x-yt-input-format",
                    "x-yt-input-format0",
                    "x-yt-input-format-0",
                    "x-yt-output-format",
                    "x-yt-output-format0",
                    "x-yt-output-format-0",
                    "x-yt-header-format",
                    "x-yt-suppress-redirect",
                    "x-yt-omit-trailers",
                ];
                rsp.setHeader("Access-Control-Allow-Headers", cors_headers.join(","));
                rsp.setHeader("Access-Control-Max-Age", "3600");
                return void utils.dispatchAs(rsp);
            }
        }

        next();
    };
};
