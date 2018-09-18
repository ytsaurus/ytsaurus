var crypto = require("crypto");

var YtRegistry = require("../registry").that;
var YtError = require("../error").that;

var utils = require("../utils");

////////////////////////////////////////////////////////////////////////////////

exports.that = function Middleware__YtLogSocket()
{
    var logger = YtRegistry.get("logger");

    function getSocketMeta(socket, with_tls) {
        var meta = {
            socket_id: socket.uuid,
            request_id: socket.last_request_id,
            remote_address: socket.remoteAddress,
            remote_port: socket.remotePort,
            local_address: socket.localAddress,
            local_port: socket.localPort
        };

        if (with_tls) {
            meta.authorized = socket.authorized;
            meta.authorizationError = socket.authorizationError;
            meta.peer_certificate = socket.getPeerCertificate && socket.getPeerCertificate();
            meta.cipher = socket.getCipher && socket.getCipher();
        }

        return meta;
    }

    return function(socket) {
        socket.uuid_ui64 = crypto.pseudoRandomBytes(8);
        socket.uuid = socket.uuid_ui64.toString("hex");

        logger.debug("New connection was established", getSocketMeta(socket, true));

        socket.once("close", function() {
            logger.debug("Connection was closed", getSocketMeta(socket));
        });

        socket.once("timeout", function() {
            logger.error("Socket timed out", getSocketMeta(socket));
        });

        socket.once("error", function(err) {
            var error = new YtError("Socket emitted an error", err);
            var meta = getSocketMeta(socket); meta.error = error.toJson();
            logger.error(error.message, meta);
        });
    };
};
