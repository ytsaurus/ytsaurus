var buffertools = require("buffertools");
var querystring = require("querystring");
var url = require("url");
var Q = require("q");

var YtError = require("./error").that;
var utils = require("./utils");

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("U", "Upravlyator");

////////////////////////////////////////////////////////////////////////////////

function YtApplicationUpravlyator(config, logger, driver)
{
    "use strict";

    this.config = config;
    this.logger = logger;
    this.driver = driver;

    this.managed_users = {};
    this.managed_groups = {};

    if (this.config.cache) {
        this.reload_interval_id = setInterval(
            this._reloadManaged.bind(this),
            this.config.reload_interval * 1000);
        // Do not prevent event loop from exiting.
        this.reload_interval_id.unref && this.reload_interval_id.unref();

        // Fire initial reload.
        this._reloadManaged();
    }
}

YtApplicationUpravlyator.prototype._getFromYt = function(type, name)
{
    "use strict";

    return Q
    .when(this.driver.executeSimple(
        "get",
        { path: "//sys/" + type + "/" + utils.escapeYPath(name) + "/@" }))
    .fail(function(error) {
        if (error.code === 500) {
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

    return Q
    .when(this._getFromYt("users", name))
    .then(function(user) {
        if (typeof(user) === "undefined") {
            return Q.reject(new YtError(
                "No such user: " + JSON.stringify(name))
                .withAttribute("is_fatal", 1));
        }
        if (user.upravlyator_managed !== "true") {
            return Q.reject(new YtError(
                "User " + JSON.stringify(name) +
                " is not managed by Upravlyator")
                .withAttribute("is_fatal", 1));
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

    return Q
    .when(this._getFromYt("groups", name))
    .then(function(group) {
        if (typeof(group) === "undefined") {
            return Q.reject(new YtError(
                "No such group: " + JSON.stringify(name))
                .withAttribute("is_fatal", 1));
        }
        if (group.upravlyator_managed !== "true") {
            return Q.reject(new YtError(
                "Group " + JSON.stringify(name) +
                " is not managed by Upravlyator")
                .withAttribute("is_fatal", 1));
        }
        return group;
    });
};

YtApplicationUpravlyator.prototype._getManagedUsers = function(force)
{
    "use strict";
    if (this.config.cache && !force) {
        return this.managed_users;
    }

    var logger = this.logger;

    return Q
    .when(this.driver.executeSimple("list", {
        path: "//sys/users",
        attributes: [
            "upravlyator_managed",
            "member_of"
        ]
    }))
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

        total = users.total;
        managed = result.length;

        logger.debug(
            "Successfully loaded list of managed users",
            { total: total, managed: managed });
        return result;
    },
    function(err) {
        var error = new YtError("Failed to load list of managed users", err);
        // XXX(sandello): Embed.
        logger.info(error.message, { error: error.toJson() });
        return Q.reject(error);
    });
};

YtApplicationUpravlyator.prototype._getManagedGroups = function()
{
    "use strict";
    if (this.config.cache && !force) {
        return this.managed_groups;
    }

    var logger = this.logger;

    return Q
    .when(this.driver.executeSimple("list", {
        path: "//sys/groups",
        attributes: [
            "upravlyator_managed",
            "upravlyator_name",
            "upravlyator_help"
        ]
    }))
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
        var error = new YtError("Failed to load list of managed groups", err);
        // XXX(sandello): Embed.
        logger.info(error.message, { error: error.toJson() });
        return Q.reject(error);
    });
};

YtApplicationUpravlyator.prototype._reloadManaged = function()
{
    "use strict";

    var self = this;
    self.logger.debug("Reloading lists of managed items");

    Q
    .all([ self._reloadManagedUsers(), self._reloadManagedGroups() ])
    .then(function() {
        self.logger.debug("Successfully reloaded lists of managed items");
    })
    .fail(function(err) {
        var error = YtError.ensureWrapped(err);
        self.logger.info(
            "Failed to reload lists of managed items",
            // XXX(sandello): Embed.
            { error: error.toJson() });
    })
    .end();
};

YtApplicationUpravlyator.prototype._reloadManagedUsers = function()
{
    "use strict";

    var self = this;
    self.logger.debug("Reloading list of managed users");

    Q
    .when(self._getManagedUsers(true))
    .then(
    function(users) { self.managed_users = users; },
    function(error) { })
    .end();
};

YtApplicationUpravlyator.prototype._reloadManagedGroups = function()
{
    "use strict";

    var self = this;
    self.logger.debug("Reloading list of managed groups");

    Q
    .when(self._getManagedGroups(true))
    .then(
    function(groups) { self.managed_groups = groups; },
    function(error) { })
    .end();
};

YtApplicationUpravlyator.prototype.dispatch = function(req, rsp, next)
{
    "use strict";

    this.logger.debug("Upravlyator call on '" + req.url + "'");

    try {
        switch (url.parse(req.url).pathname) {
            case "/info":
                return this._dispatchInfo(req, rsp);
            case "/add-role":
                return this._dispatchAddRole(req, rsp);
            case "/remove-role":
                return this._dispatchRemoveRole(req, rsp);
            case "/get-user-roles":
                return this._dispatchGetUserRoles(req, rsp);
            case "/get-all-roles":
                return this._dispatchGetAllRoles(req, rsp);
        }
        throw new YtError("Unknown URI");
    } catch(err) {
        return this._dispatchError(YtError.ensureWrapped(err));
    }
};

YtApplicationUpravlyator.prototype._dispatchError = function(req, rsp, error)
{
    "use strict";
    if (error.isOK()) {
        return;
    }
    var body = { code: error.code };
    if (error.attributes.is_fatal) {
        body.fatal = (req.uuid ? req.uuid + ": " : "") + error.message;
    } else {
        body.error = (req.uuid ? req.uuid + ": " : "") + error.message;
    }
    utils.dispatchJson(rsp, body);
};

YtApplicationUpravlyator.prototype._dispatchInfo = function(req, rsp)
{
    "use strict";

    var self = this;

    Q
    .when(self._getManagedGroups())
    .then(function(groups) {
        utils.dispatchJson(rsp, {
            code: 0,
            roles: {
                slug: "group",
                name: "Группа",
                values: groups
            }
        });
    })
    .fail(function(err) {
        var error = YtError.ensureWrapped(err);
        self._dispatchError(req, rsp, error);
    })
    .end();
};

YtApplicationUpravlyator.prototype._dispatchAddRole = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    Q
    .when(self._extractUserGroup(req, rsp))
    .spread(function(user, group) {
        var user_name = user.name;
        var group_name = group.name;
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { user: user_name, group: group_name });

        if (user.member_of.indexOf(group_name) !== -1) {
            var error = new YtError(
                "User '" + user_name + "' is already a member of group '" + group_name + "'");
            tagged_logger.debug(error.message);
            utils.dispatchJson(rsp, { code: 0, warning: error.message });
            return Q.reject(new YtError());
        }

        tagged_logger.debug("Adding Upravlyator role");
        var membership = self.driver.executeSimple(
            "add_member",
            { member: user_name, group: group_name });
        return Q.all([ tagged_logger, membership ]);
    })
    .spread(function(tagged_logger, membership) {
        tagged_logger.debug("Successfully added Upravlyator role");
        utils.dispatchJson(rsp, { code: 0 });
    })
    .fail(function(err) {
        var error = YtError.ensureWrapped(err);
        if (error.code !== 0) {
            logger.info(
                "Failed to add Upravlyator role",
                // XXX(sandello): Embed.
                { error: error.toJson() });
        }
        self._dispatchError(req, rsp, error);
    })
    .end();
};

YtApplicationUpravlyator.prototype._dispatchRemoveRole = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    Q
    .when(self._extractUserGroup(req, rsp))
    .spread(function(user, group) {
        var user_name = user.name;
        var group_name = group.name;
        var tagged_logger = new utils.TaggedLogger(
            logger,
            { user: user_name, group: group_name });

        if (user.member_of.indexOf(group_name) === -1) {
            var error = new YtError(
                "User '" + user_name + "' is already not a member of group '" + group_name + "'");
            tagged_logger.debug(error.message);
            utils.dispatchJson(rsp, { code: 0, warning: error.message });
            return Q.reject(new YtError());
        }

        tagged_logger.debug("Removing Upravlyator role");
        var membership = self.driver.executeSimple(
            "remove_member",
            { member: user_name, group: group_name });
        return Q.all([ tagged_logger, membership ]);
    })
    .spread(function(tagged_logger, membership) {
        tagged_logger.debug("Successfully removed Upravlyator role");
        utils.dispatchJson(rsp, { code: 0 });
    })
    .fail(function(err) {
        if (!err) {
            return;
        }
        var error = YtError.ensureWrapped(err);
        if (error.code !== 0) {
            logger.info(
                "Failed to remove Upravlyator role",
                // XXX(sandello): Embed.
                { error: error.toJson() });
        }
        self._dispatchError(req, rsp, error);
    })
    .end();
};

YtApplicationUpravlyator.prototype._dispatchGetUserRoles = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    var params = querystring.parse(url.parse(req.url).query);
    var login = params.login;

    Q
    .all([ self._getManagedUser(login), self._getManagedGroups() ])
    .spread(function(user, groups) {
        var roles = user.member_of
        .filter(function(group) {
            return groups.hasOwnProperty(group);
        })
        .map(function(group) {
            return { group: group };
        });
        utils.dispatchJson(rsp, { code: 0, roles: roles });
    })
    .fail(function(err) {
        var error = YtError.ensureWrapped(err);
        if (error.code !== 0) {
            logger.info(
                "Failed to get Upravlyator user roles",
                // XXX(sandello): Embed.
                { error: error.toJson() });
        }
        self._dispatchError(req, rsp, error);
    })
    .end();
};

YtApplicationUpravlyator.prototype._dispatchGetAllRoles = function(req, rsp)
{
    "use strict";

    var self = this;
    var logger = req.logger || self.logger;

    Q
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
    .fail(function(err) {
        var error = YtError.ensureWrapped(err);
        if (error.code !== 0) {
            logger.info(
                "Failed to get Upravlyator users and roles",
                // XXX(sandello): Embed.
                { error: error.toJson() });
        }
        self._dispatchError(req, rsp, error);
    })
    .end();
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
    var done = false;
    var logger = req.logger || self.logger;

    function failHandler(err) {
        var error = YtError.ensureWrapped(err);
        logger.info(error.message);
        return Q.reject(error);
    }

    return Q
    .when(self._captureBody(req, rsp))
    .then(function(body) {
        logger.debug("Verifying Upravlyator user and group", {
            payload: body
        });

        // These are Upravlyator terms.
        var role = JSON.parse(body.role);

        // These are YT terms.
        var user_name = body.login;
        var group_name = role.group;

        var user = self._getManagedUser(user_name).fail(failHandler);
        var group = self._getManagedGroup(group_name).fail(failHandler);

        return Q.all([ user, group ]);
    });
};

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationUpravlyator;
