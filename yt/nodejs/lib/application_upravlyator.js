var buffertools = require("buffertools");
var querystring = require("querystring");
var url = require("url");
var Q = require("bluebird");

var YtError = require("./error").that;

var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("U", "Upravlyator");

////////////////////////////////////////////////////////////////////////////////

function YtApplicationUpravlyator(logger, driver)
{
    "use strict";

    this.logger = logger;
    this.driver = driver;
}

YtApplicationUpravlyator.prototype._getFromYt = function(type, name)
{
    "use strict";

    return this.driver.executeSimple(
        "get",
        { path: "//sys/" + type + "/" + utils.escapeYPath(name) + "/@" })
    .catch(function(error) {
        if (checkForErrorCode(error, 500)) {
            return; // Resolve error, return 'undefined';
        } else {
            return Q.reject(error);
        }
    });
};

YtApplicationUpravlyator.prototype._getManagedUser = function(name)
{
    "use strict";

    if (typeof(name) !== "string") {
        return Q.reject(new YtError("User name is not specified"));
    }

    return this._getFromYt("users", name).then(function(user) {
        if (typeof(user) === "undefined") {
            return Q.reject(new YtError(
                "No such user: " + JSON.stringify(name))
                .withAttribute("is_fatal", 1)
                .withAttribute("missing", 1));
        }
        if (user.upravlyator_managed !== "true") {
            return Q.reject(new YtError(
                "User " + JSON.stringify(name) +
                " is not managed by Upravlyator")
                .withAttribute("is_fatal", 1)
                .withAttribute("unmanaged", 1));
        }
        return user;
    });
};

YtApplicationUpravlyator.prototype._getManagedGroup = function(name)
{
    "use strict";

    if (typeof(name) !== "string") {
        return Q.reject(new YtError("Group name is not specified"));
    }

    return this._getFromYt("groups", name).then(function(group) {
        if (typeof(group) === "undefined") {
            return Q.reject(new YtError(
                "No such group: " + JSON.stringify(name))
                .withAttribute("is_fatal", 1)
                .withAttribute("missing", 1));
        }
        if (group.upravlyator_managed !== "true") {
            return Q.reject(new YtError(
                "Group " + JSON.stringify(name) +
                " is not managed by Upravlyator")
                .withAttribute("is_fatal", 1)
                .withAttribute("unmanaged", 1));
        }
        return group;
    });
};

YtApplicationUpravlyator.prototype._getManagedUsers = function(force)
{
    "use strict";

    var logger = this.logger;

    return this.driver.executeSimple("list", {
        path: "//sys/users",
        attributes: [
            "upravlyator_managed",
            "member_of"
        ]
    })
    .then(
    function(users) {
        var total, managed;
        var result;

        result = users
        .filter(function(user) {
            return utils.getYsonAttribute(user, "upravlyator_managed") === "true";
        })
        .map(function(user) {
            var value = utils.getYsonValue(user);
            var member_of = utils.getYsonAttribute(user, "member_of");
            return { name: value, member_of: member_of };
        });

        total = users.length;
        managed = result.length;

        logger.debug(
            "Successfully loaded list of managed users",
            { total: total, managed: managed });
        return result;
    },
    function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to load list of managed users"));
    });
};

YtApplicationUpravlyator.prototype._getManagedGroups = function()
{
    "use strict";

    var logger = this.logger;

    return this.driver.executeSimple("list", {
        path: "//sys/groups",
        attributes: [
            "upravlyator_managed",
            "upravlyator_name",
            "upravlyator_help"
        ]
    })
    .then(
    function(groups) {
        var total, managed;
        var result = {};

        groups
        .filter(function(group) {
            return utils.getYsonAttribute(group, "upravlyator_managed") === "true";
        })
        .forEach(function(group) {
            var value = utils.getYsonValue(group);
            var name = utils.getYsonAttribute(group, "upravlyator_name");
            var help = utils.getYsonAttribute(group, "upravlyator_help");
            result[value] = { name: name || value, help: help };
        });

        total = groups.length;
        managed = Object.keys(result).length;

        logger.debug(
            "Successfully loaded list of managed groups",
            { total: total, managed: managed });
        return result;
    },
    function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to load list of managed groups"));
    });
};

YtApplicationUpravlyator.prototype.dispatch = function(req, rsp, next)
{
    "use strict";

    var self = this;
    self.logger.debug("Upravlyator call on '" + req.url + "'");

    if (req.method === "POST") {
        req.body = self._captureBody(req, rsp);
    }

    return Q.try(function() {
        switch (url.parse(req.url).pathname) {
            case "/info":
                return self._dispatchInfo(req, rsp);
            case "/add-role":
                return self._dispatchAddRole(req, rsp);
            case "/remove-role":
                return self._dispatchRemoveRole(req, rsp);
            case "/get-user-roles":
                return self._dispatchGetUserRoles(req, rsp);
            case "/get-all-roles":
                return self._dispatchGetAllRoles(req, rsp);
        }
        throw new YtError("Unknown URI");
    }).catch(self._dispatchError.bind(self, req, rsp));
};

YtApplicationUpravlyator.prototype._dispatchError = function(req, rsp, err)
{
    "use strict";

    var error = YtError.ensureWrapped(err);
    var logger = req.logger || this.logger;

    var message = error.message;

    var body = { code: error.code };
    var type = "warning";

    if (!error.isOK()) {
        type = error.attributes.is_fatal ? "fatal" : "error";
    }

    if (message) {
        body[type] = (req.uuid ? req.uuid + ": " : "") + message;
        logger.info("Error was caught in ApplicationUpravlyator", {
            // TODO(sandello): Embed.
            error: error.toJson()
        });
    }

    return utils.dispatchJson(rsp, body);
};

YtApplicationUpravlyator.prototype._dispatchInfo = function(req, rsp)
{
    "use strict";

    return this._getManagedGroups().then(function(groups) {
        return utils.dispatchJson(rsp, {
            code: 0,
            roles: {
                slug: "group",
                name: "Группа",
                values: groups
            }
        });
    });
};

YtApplicationUpravlyator.prototype._dispatchAddRole = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    return self._extractUserGroup(req, rsp)
    .spread(function(user, group) {
        var user_name = user.name;
        var group_name = group.name;
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { user: user_name, group: group_name });

        if (user.member_of.indexOf(group_name) !== -1) {
            return Q.reject(
                new YtError(
                    "User '" + user_name +
                    "' is already a member of group '" + group_name + "'")
                .withCode(0));
        }

        tagged_logger.debug("Adding Upravlyator role");
        var membership = self.driver.executeSimple(
            "add_member",
            { member: user_name, group: group_name });
        return Q.all([ tagged_logger, membership ]);
    })
    .spread(function(tagged_logger, membership) {
        tagged_logger.debug("Successfully added Upravlyator role");
        return utils.dispatchJson(rsp, { code: 0 });
    })
    .catch(function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to add Upravlyator role"));
    });
};

YtApplicationUpravlyator.prototype._dispatchRemoveRole = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    return self._extractUserGroup(req, rsp)
    .spread(function(user, group) {
        var user_name = user.name;
        var group_name = group.name;
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { user: user_name, group: group_name });

        if (user.member_of.indexOf(group_name) === -1) {
            return Q.reject(
                new YtError(
                    "User '" + user_name +
                    "' is already not a member of group '" + group_name + "'")
                .withCode(0));
        }

        tagged_logger.debug("Removing Upravlyator role");
        var membership = self.driver.executeSimple(
            "remove_member",
            { member: user_name, group: group_name });
        return Q.all([ tagged_logger, membership ]);
    })
    .spread(function(tagged_logger, membership) {
        tagged_logger.debug("Successfully removed Upravlyator role");
        return utils.dispatchJson(rsp, { code: 0 });
    })
    .catch(function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to remove Upravlyator role"));
    });
};

YtApplicationUpravlyator.prototype._dispatchGetUserRoles = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    var params = querystring.parse(url.parse(req.url).query);
    var login = params.login;

    // TODO(sandello): This is hacky. Fix me?
    var maybe_user = self._getManagedUser(login).catch(function(err) {
        var error = YtError.ensureWrapped(err);
        if (error.attributes.missing || error.attributes.unmanaged) {
            return;
        } else {
            return Q.reject(error);
        }
    });

    return Q
    .all([ maybe_user, self._getManagedGroups() ])
    .spread(function(user, groups) {
        if (typeof(user) === "undefined") {
            return utils.dispatchJson(rsp, { code: 0, roles: [] });
        }
        var roles = user.member_of
        .filter(function(group) {
            return groups.hasOwnProperty(group);
        })
        .map(function(group) {
            return { group: group };
        });
        utils.dispatchJson(rsp, { code: 0, roles: roles });
    })
    .catch(function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to get Upravlyator user roles"));
    });
};

YtApplicationUpravlyator.prototype._dispatchGetAllRoles = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    return Q
    .all([ self._getManagedUsers(), self._getManagedGroups() ])
    .spread(function(users, groups) {
        users = users.map(function(user) {
            var login = user.name;
            var roles = user.member_of
            .filter(function(group) {
                return groups.hasOwnProperty(group);
            })
            .map(function(group) {
                return { group: group };
            });
            return { login: login, roles: roles };
        });
        utils.dispatchJson(rsp, { code: 0, users: users });
    })
    .catch(function(err) {
        return Q.reject(YtError.ensureWrapped(
            err, "Failed to get Upravlyator users and roles"));
    });
};

YtApplicationUpravlyator.prototype._captureBody = function(req, rsp)
{
    "use strict";

    var deferred = Q.defer();
    var chunks = [];

    req.on("data", function(chunk) { chunks.push(chunk); });
    req.on("end", function() {
        try {
            var body = buffertools.concat.apply(undefined, chunks);
            var result = querystring.parse(body.toString("utf-8"));
            deferred.resolve(result);
        } catch (err) {
            deferred.reject(err);
        }
    });

    return deferred.promise;
};

YtApplicationUpravlyator.prototype._extractUserGroup = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    function tracer(err) {
        var error = YtError.ensureWrapped(err);
        logger.info(error.message);
        return Q.reject(error);
    }

    return Q.cast(req.body).then(function(body) {
        logger.debug("Verifying Upravlyator user and group", {
            payload: body
        });

        // These are Upravlyator terms.
        var role = JSON.parse(body.role);

        // These are YT terms.
        var user_name = body.login;
        var group_name = role.group;

        var user = self._getManagedUser(user_name).catch(tracer);
        var group = self._getManagedGroup(group_name).catch(tracer);

        return Q.all([ user, group ]);
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationUpravlyator;
