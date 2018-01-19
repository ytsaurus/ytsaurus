var url = require("url");
var util = require("util");

var Q = require("bluebird");
var _ = require("underscore");
var UI64 = require("cuint").UINT64;

var binding = process._linkedBinding ? process._linkedBinding("ytnode") : require("./ytnode");
var utils = require("./utils");

var YtError = require("./error").that;
var YtHttpRequest = require("./http_request").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("V", "Operations");

////////////////////////////////////////////////////////////////////////////////

var OPERATIONS_ARCHIVE_DIRECTORY = "//sys/operations_archive";
var OPERATIONS_ARCHIVE_PATH = "{}/ordered_by_id".format(OPERATIONS_ARCHIVE_DIRECTORY);
var OPERATIONS_ARCHIVE_INDEX_PATH = "{}/ordered_by_start_time".format(OPERATIONS_ARCHIVE_DIRECTORY);
var OPERATIONS_CYPRESS_PATH = "//sys/operations";
var OPERATIONS_RUNTIME_PATH = "//sys/scheduler/orchid/scheduler/operations";
var SCHEDULING_INFO_PATH = "//sys/scheduler/orchid/scheduler";
var MAX_SIZE_LIMIT = 100;
var TIME_SPAN_DAY = 24 * 3600 * 1000000;
var TIME_SPAN_LIMIT = 10 * TIME_SPAN_DAY;
var CYPRESS_OPERATIONS_SUCCESS_EXPIRATION_TIME = 3000;
var CYPRESS_OPERATIONS_FAILURE_EXPIRATION_TIME = 1000;

var INTERMEDIATE_STATES = [
    "initializing",
    "preparing",
    "reviving",
    "completing",
    "aborting",
    "failing"
];

var POOL_FIELDS = [
    "parent",
    "pool",
    "fair_share_ratio",
    "usage_ratio",
    "min_share_ratio",
    "max_share_ratio",
    "weight",
    "starving",
    "satisfaction_ratio",
    "dominant_resource",
    "resource_usage",
    "resource_limits",
    "resource_demand",
    "demand_ratio"
];

var OPERATION_ATTRIBUTES = [
    "authenticated_user",
    "brief_progress",
    "brief_spec",
    "finish_time",
    "operation_type",
    "start_time",
    "state",
    "suspended",
    "title",
    "weight"
];

var ANNOTATED_JSON_FORMAT = {
    $value: "json",
    $attributes: {
        annotate_with_types: "true",
        stringify: "true"
    }
};

var SLOW_QUERY_TIMEOUT = 3000;

function mapState(state)
{
    return INTERMEDIATE_STATES.indexOf(state) !== -1 ? "running" : state;
}

function extractTextFactorForCypressItem(value, attributes)
{
    var factors = [];
    factors.push(value);
    factors.push(attributes.authenticated_user);
    factors.push(attributes.state);
    factors.push(attributes.operation_type);
    factors.push(attributes.pool);
    var brief_spec = attributes.brief_spec;
    if (typeof(brief_spec) === "object") {
        factors.push(brief_spec.title);
        if (brief_spec.input_table_paths) {
            factors.push(utils.getYsonValue(utils.getYsonValue(brief_spec.input_table_paths)[0]));
        }
        if (brief_spec.output_table_paths) {
            factors.push(utils.getYsonValue(utils.getYsonValue(brief_spec.output_table_paths)[0]));
        }
    }
    factors = factors.filter(function(factor) { return !!factor; });
    return factors.join(" ").toLowerCase();
}

function escapeC(string)
{
    return binding.EscapeC(string);
}

function stripJsonAnnotations(annotated_json)
{
    if (_.isArray(annotated_json)) {
        return _.map(annotated_json, stripJsonAnnotations);
    } else if (_.isObject(annotated_json)) {
        if (!_.has(annotated_json, "$value")) {
            return _.mapObject(annotated_json, stripJsonAnnotations);
        }

        var value = annotated_json.$value;
        var type = annotated_json.$type;

        if (type === "int64" || type === "uint64" || type === "double") {
            value = +value;
        } else if (type === "boolean") {
            if (!_.isBoolean(annotated_json)) {
                value = value === "true" ? true : false;
            }
        }

        if (!_.has(annotated_json, "$attributes") && !_.has(annotated_json, "$incomplete")) {
            return value;
        }

        return {
            $attributes: stripJsonAnnotations(annotated_json.$attributes),
            $incomplete: annotated_json.$incomplete,
            $value: stripJsonAnnotations(value)
        };
    } else {
        return annotated_json;
    }
}

function tidyArchiveOperation(operation)
{
    var keys = [
        "id_hi",
        "id_lo",
        "id_hash",
        "filter_factors",
        "index.id_hi",
        "index.id_lo",
        "index.start_time",
        "index.dummy"
    ];

    _.each(keys, function(key) {
        delete operation[key];
    });

    operation.is_archived = true;
    if (operation.start_time) {
        operation.start_time = utils.microsToUtcString(utils.getYsonValue(operation.start_time));
    }
    if (operation.finish_time) {
        operation.finish_time = utils.microsToUtcString(utils.getYsonValue(operation.finish_time));
    }

    return operation;
}

function idUint64ToStringNew(id_hi, id_lo)
{
    var hi, lo, mask, parts;
    hi = id_hi instanceof UI64 ? id_hi : UI64(id_hi, 10);
    lo = id_lo instanceof UI64 ? id_lo : UI64(id_lo, 10);
    mask = UI64(1).shiftLeft(UI64(32)).subtract(UI64(1));
    parts = [
        lo.clone().shiftRight(UI64(32)).toString(16),
        lo.clone().and(mask).toString(16),
        hi.clone().shiftRight(UI64(32)).toString(16),
        hi.clone().and(mask).toString(16)];
    return parts.join("-");
}

function idStringToUint64New(id)
{
    var hi, lo, parts;
    parts = id.split("-");
    hi = UI64(parts[2], 16).shiftLeft(32).or(UI64(parts[3], 16));
    lo = UI64(parts[0], 16).shiftLeft(32).or(UI64(parts[1], 16));
    return [hi, lo];
}

function idUint64ToString(id_hi, id_lo)
{
    var hi, lo, mask, parts;
    hi = id_hi instanceof UI64 ? id_hi : UI64(id_hi, 10);
    lo = id_lo instanceof UI64 ? id_lo : UI64(id_lo, 10);
    mask = UI64(1).shiftLeft(UI64(32)).subtract(UI64(1));
    parts = [
        lo.clone().and(mask).toString(16),
        lo.clone().shiftRight(UI64(32)).toString(16),
        hi.clone().and(mask).toString(16),
        hi.clone().shiftRight(UI64(32)).toString(16)];
    return parts.join("-");
}

function idStringToUint64(id)
{
    var hi, lo, parts;
    parts = id.split("-");
    hi = UI64(parts[3], 16).shiftLeft(32).or(UI64(parts[2], 16));
    lo = UI64(parts[1], 16).shiftLeft(32).or(UI64(parts[0], 16));
    return [hi, lo];
}

function validateString(value)
{
    if (typeof(value) === "string") {
        return value;
    }
    throw new YtError("Unable to parse string")
        .withCode(1)
        .withAttribute("value", escapeC(value + ""));
}

function validateId(value)
{
    value = validateString(value);
    if (/^[0-9a-f]{1,8}-[0-9a-f]{1,8}-[0-9a-f]{1,8}-[0-9a-f]{1,8}$/i.test(value)) {
        return value;
    }
    throw new YtError("Unable to parse operation id")
        .withCode(1)
        .withAttribute("value", escapeC(value + ""));
}

function validateBoolean(value)
{
    if (value === true || value === false) {
        return value;
    } else if (typeof(value) === "string") {
        if (value === "true") {
            return true;
        } else if (value === "false") {
            return false;
        }
    }
    throw new YtError("Unable to parse boolean")
        .withCode(1)
        .withAttribute("value", escapeC(value + ""));
}

function validateInteger(value)
{
    if (typeof(value) === "number") {
        return ~~value;
    } else if (typeof(value) === "string") {
        var parsed = parseInt(value);
        if (!isNaN(parsed)) {
            return parsed;
        }
    }
    throw new YtError("Unable to parse integer")
        .withCode(1)
        .withAttribute("value", escapeC(value + ""));
}

function validateDateTime(value)
{
    if (typeof(value) === "string" && !isNaN(Date.parse(value))) {
        return utils.utcStringToMicros(value);
    }
    throw new YtError("Unable to parse datetime")
        .withCode(1)
        .withAttribute("value", escapeC(value + ""));
}

function optional(parameters, key, validator, default_value)
{
    if (_.has(parameters, key) && typeof(parameters[key]) !== "undefined") {
        return validator(parameters[key]);
    } else {
        if (default_value) {
            return validator(default_value);
        } else {
            return null;
        }
    }
}

function required(parameters, key, validator)
{
    var result = optional(parameters, key, validator);
    if (result !== null) {
        return result;
    } else {
        throw new YtError("Missing required parameter \"" + key + "\"")
            .withCode(1);
    }
}

function getArchiveCallbacks(
    timings,
    from_time, to_time, cursor_time, cursor_direction,
    user_filter, state_filter, type_filter, substr_filter,
    pool_filter, max_size,
    version)
{
    if (pool_filter !== null && version < 15) {
        makeErrorHandler("Failed to get operation's pool: operations archive version is too old: expected >= 15, got %v", version);
    }

    var start_time_name = version < 2 ? "index.start_time" : "start_time";
    var counts_filter_conditions = [
        "{} > {} AND {} <= {}".format(start_time_name, from_time, start_time_name, to_time)
    ];

    if (substr_filter) {
        counts_filter_conditions.push(
            "is_substr(\"{}\", filter_factors)".format(escapeC(substr_filter)));
    }
    var items_filter_conditions = counts_filter_conditions.slice();
    var items_sort_direction;

    if (cursor_direction === "past") {
        if (cursor_time !== null) {
            items_filter_conditions.push("{} <= {}".format(start_time_name, cursor_time));
        }
        items_sort_direction = "DESC";
    }

    if (cursor_direction === "future") {
        if (cursor_time !== null) {
            items_filter_conditions.push("{} > {}".format(start_time_name, cursor_time));
        }
        items_sort_direction = "ASC";
    }

    if (pool_filter) {
        items_filter_conditions.push("pool = \"{}\"".format(escapeC(pool_filter)));
    }

    if (state_filter) {
        items_filter_conditions.push("state = \"{}\"".format(escapeC(state_filter)));
    }

    if (type_filter) {
        items_filter_conditions.push("operation_type = \"{}\"".format(escapeC(type_filter)));
    }

    if (user_filter) {
        items_filter_conditions.push("authenticated_user = \"{}\"".format(escapeC(user_filter)));
    }

    var query_source = null;
    if (version < 2) {
        query_source = "[{}] index JOIN [{}] ON (index.id_hi, index.id_lo) = (id_hi, id_lo)"
            .format(OPERATIONS_ARCHIVE_INDEX_PATH, OPERATIONS_ARCHIVE_PATH);
    } else {
        query_source = "[{}]"
            .format(OPERATIONS_ARCHIVE_INDEX_PATH);
    }

    var pool_column_name = version < 15 ? "''" : "pool";

    var query_for_counts =
        "pool, user, state, type, sum(1) AS count FROM {}".format(query_source) +
        " WHERE {}".format(counts_filter_conditions.join(" AND ")) +
        " GROUP BY {} AS pool, authenticated_user AS user, state AS state, operation_type AS type".format(pool_column_name);

    var query_for_items =
        "{} FROM {}".format(version < 2 ? "*" : "id_hi, id_lo", query_source) +
        " WHERE {}".format(items_filter_conditions.join(" AND ")) +
        " ORDER BY start_time {}".format(items_sort_direction) +
        " LIMIT {}".format(1 + max_size);

    var logger = this.logger;
    var driver = this.driver;

    function makeErrorHandler(message, meta)
    {
        return function(err) {
            var error = YtError.ensureWrapped(err);
            var cloned_meta = _.clone(meta || {});
            cloned_meta.error = error.toJson();
            logger.error(message, cloned_meta);
            return Q.reject(error);
        };
    }

    function makeSlowOpHandler(key, ts, meta)
    {
        return function() {
            var dt = timings[key] = new Date() - ts;
            if (dt > SLOW_QUERY_TIMEOUT) {
                var cloned_meta = _.clone(meta || {});
                cloned_meta.time = dt;
                logger.debug("Slow operation '" + key + "'", cloned_meta);
            }
        };
    }

    return {
        getCounts: function() {
            var timing_start = new Date();
            return driver.executeSimple(
                "select_rows",
                {query: query_for_counts})
                .catch(makeErrorHandler("Failed to select operation counts from archive index", {query: query_for_counts}))
                .finally(makeSlowOpHandler("archive_counts", timing_start, {query: query_for_counts}));
        },
        getItems: function() {
            var timing_start = new Date();
            if (version < 2) {
                return driver.executeSimple(
                    "select_rows",
                    {query: query_for_items, output_format: ANNOTATED_JSON_FORMAT})
                    .catch(makeErrorHandler("Failed to select operations from archive", {query: query_for_items}))
                    .finally(makeSlowOpHandler("archive_items", timing_start, {query: query_for_items}));
            } else {
                var timing_start_interim;

                var archive_item_ids = driver
                    .executeSimple(
                        "select_rows",
                        {query: query_for_items, output_format: ANNOTATED_JSON_FORMAT})
                    .then(function(value) {
                        timing_start_interim = new Date();
                        return value;
                    })
                    .catch(makeErrorHandler("Failed to select operations from archive index", {query: query_for_items}))
                    .finally(makeSlowOpHandler("archive_item_select", timing_start, {query: query_for_items}));

                var archive_items = archive_item_ids.then(driver.executeSimple.bind(driver, "lookup_rows", {
                        path: OPERATIONS_ARCHIVE_PATH,
                        input_format: ANNOTATED_JSON_FORMAT,
                        output_format: ANNOTATED_JSON_FORMAT}))
                    .catch(makeErrorHandler("Failed to lookup operations from archive"))
                    .finally(makeSlowOpHandler("archive_items_lookup", timing_start_interim));

                return archive_items;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////

function YtApplicationOperations(logger, driver)
{
    this.logger = logger;
    this.driver = driver;
}

YtApplicationOperations._idUint64ToString = idUint64ToString;
YtApplicationOperations._idStringToUint64 = idStringToUint64;
YtApplicationOperations._idUint64ToStringNew = idUint64ToStringNew;
YtApplicationOperations._idStringToUint64New = idStringToUint64New;

YtApplicationOperations.prototype.makeErrorHandler = Q.method(
function makeErrorHandler(message)
{
    var logger = this.logger;
    return function(error) {
        var err = YtError.ensureWrapped(error);
        logger.error(message, {error: err.toJson()});
        return Q.reject(new YtError(message, err));
    };
});

YtApplicationOperations.prototype.getVersion = Q.method(
function getVersion()
{
    return this.driver.executeSimple(
        "get",
        {
            path: "{}/@version".format(OPERATIONS_ARCHIVE_DIRECTORY)
        })
        .catch(function(error) {
            var err = YtError.ensureWrapped(error);
            return err.getCode() === 500;
        }, function(error) {
            return 0;
        })
        .catch(this.makeErrorHandler("Failed to fetch archive version"));
});

YtApplicationOperations.prototype.list = Q.method(
function YtApplicationOperations$list(parameters)
{
    var from_time = optional(parameters, "from_time", validateDateTime);
    var to_time = optional(parameters, "to_time", validateDateTime);
    var cursor_time = optional(parameters, "cursor_time", validateDateTime);
    var cursor_direction = optional(parameters, "cursor_direction", validateString);
    var user_filter = optional(parameters, "user", validateString);
    var state_filter = optional(parameters, "state", validateString);
    var type_filter = optional(parameters, "type", validateString);
    var substr_filter = optional(parameters, "filter", validateString);
    var pool_filter = optional(parameters, "pool", validateString);
    var with_failed_jobs = optional(parameters, "with_failed_jobs", validateBoolean, false);
    var include_archive = optional(parameters, "include_archive", validateBoolean, false);
    var include_counters = optional(parameters, "include_counters", validateBoolean, true);
    var max_size = optional(parameters, "max_size", validateInteger, MAX_SIZE_LIMIT);

    // Process |from_time| & |to_time|.
    if (include_archive) {
        if (from_time === null) {
            throw new YtError("Missing required parameter \"from_time\"")
                .withCode(1);
        }
        if (to_time === null) {
            throw new YtError("Missing required parameter \"to_time\"")
                .withCode(1);
        }
        var time_span = to_time - from_time;
        if (time_span > TIME_SPAN_LIMIT) {
            throw new YtError("Time span exceedes allowed limit ({} days > {} days)".format(
                time_span / TIME_SPAN_DAY, TIME_SPAN_LIMIT / TIME_SPAN_DAY)).withCode(1);
        }
    }

    // Process |cursor_time|, |cursor_direction|.
    if (cursor_time !== null && (
        to_time !== null && cursor_time > to_time ||
        from_time !== null && cursor_time < from_time))
    {
        throw new YtError("Time cursor is out of range").withCode(1);
    }

    if (cursor_direction === null) {
        cursor_direction = "past";
    } else {
        cursor_direction = cursor_direction.toLowerCase();
    }

    if (cursor_direction !== "past" && cursor_direction !== "future") {
        throw new YtError("Cursor direction must be either 'past' of 'future'").withCode(1);
    }

    // TODO(sandello): Validate |state_filter|, |type_filter|.

    // Process |substr_filter|.
    if (substr_filter !== null) {
        substr_filter = substr_filter.toLowerCase();
    }

    // Process |max_size|.
    if (max_size > MAX_SIZE_LIMIT) {
        throw new YtError("Maximum result size exceedes allowed limit ({} > {})".format(
            max_size, MAX_SIZE_LIMIT)).withCode(1);
    }

    var timings = {};

    // Okay, now fetch & merge data.
    var timings_start = new Date();
    var cypress_data = this.driver.executeSimple(
        "list",
        {
            path: OPERATIONS_CYPRESS_PATH,
            attributes: OPERATION_ATTRIBUTES,
            read_from: "cache",
            expire_after_successful_update_time: CYPRESS_OPERATIONS_SUCCESS_EXPIRATION_TIME,
            expire_after_failed_update_time: CYPRESS_OPERATIONS_FAILURE_EXPIRATION_TIME
        })
        .catch(this.makeErrorHandler("Failed to fetch operations from Cypress"))
        .finally(function() {
            timings.cypress_data = new Date() - timings_start;
        });

    var version = this.getVersion()
        .finally(function() {
            timings.version = new Date() - timings_start;
        });

    var archive_counts = Q.resolve([]);
    var archive_data = Q.resolve([]);

    if (include_archive) {
        var archive_callbacks = version.then(getArchiveCallbacks.bind(
            this,
            timings,
            from_time,
            to_time,
            cursor_time,
            cursor_direction,
            user_filter,
            state_filter,
            type_filter,
            substr_filter,
            pool_filter,
            max_size));

        if (include_counters) {
            archive_counts = archive_callbacks.then(function(callbacks) {
                return callbacks.getCounts();
            });
        }

        archive_data = archive_callbacks.then(function(callbacks) {
            return callbacks.getItems();
        });
    }

    function makeRegister() {
        var pool_counts = {};
        var user_counts = {};
        var state_counts = {};
        var type_counts = {};

        return {
            filterAndCount: function(pool, user, state, type, count) {
                // POOL
                if (!pool_counts.hasOwnProperty(pool)) {
                    pool_counts[pool] = 0;
                }
                pool_counts[pool] += count;

                if (pool_filter && pool !== pool_filter) {
                    return false;
                }

                // USER
                if (!user_counts.hasOwnProperty(user)) {
                    user_counts[user] = 0;
                }
                user_counts[user] += count;

                if (user_filter && user !== user_filter) {
                    return false;
                }

                // STATE
                if (!state_counts.hasOwnProperty(state)) {
                    state_counts[state] = 0;
                }
                state_counts[state] += count;

                if (state_filter && state !== state_filter) {
                    return false;
                }

                // TYPE
                if (!type_counts.hasOwnProperty(type)) {
                    type_counts[type] = 0;
                }
                type_counts[type] += count;

                if (type_filter && type !== type_filter) {
                    return false;
                }

                return true;
            },
            result: {
                pool_counts: pool_counts,
                user_counts: user_counts,
                state_counts: state_counts,
                type_counts: type_counts,
            }
        };
    }

    if (include_archive && include_counters) {
        archive_counts = archive_counts
            .catch(this.makeErrorHandler("Failed to fetch operation counts from archive"));
    }

    if (include_archive) {
        archive_data = archive_data
            .catch(this.makeErrorHandler("Failed to fetch operation items from archive"));
    }

    var logger = this.logger;

    return Q.settle([version, cypress_data, archive_data, archive_counts])
    .spread(function(version, cypress_data, archive_data, archive_counts) {
        version = version.value();

        if (cypress_data.isRejected()) {
            return Q.reject(cypress_data.error());
        } else {
            cypress_data = cypress_data.value();

            filtered_cypress_data = [];
            for (var i = 0; i < cypress_data.length; i++) {
                var attributes = utils.getYsonAttributes(cypress_data[i]);
                if ("state" in attributes) {
                    filtered_cypress_data.push(cypress_data[i]);
                }
            }
            cypress_data = filtered_cypress_data;
        }

        if (archive_data.isRejected()) {
            return Q.reject(archive_data.error());
        } else {
            archive_data = archive_data.value();
        }

        if (archive_counts.isRejected()) {
            return Q.reject(archive_counts.error());
        } else {
            archive_counts = archive_counts.value();
        }

        // Now, compute counts & merge data.
        var register = makeRegister();

        _.each(archive_counts, function(item) {
            register.filterAndCount(item.pool, item.user, item.state, item.type, item.count);
        });

        var failed_jobs_count = 0;

        archive_data = archive_data.map(function(operation) {
            var id = (version < 6 ? idUint64ToString : idUint64ToStringNew)(operation.id_hi.$value, operation.id_lo.$value);

            tidyArchiveOperation(operation);

            return {
                $value: id,
                $attributes: stripJsonAnnotations(operation),
            };
        });

        // Start building result with Cypress data.
        var merged_data = _.filter(cypress_data, function(item) {
            var value = utils.getYsonValue(item);
            var attributes = utils.getYsonAttributes(item);

            // Check time filter.
            var start_time = utils.utcStringToMicros(attributes.start_time);
            if ((from_time !== null && start_time < from_time) || (to_time !== null && start_time >= to_time)) {
                return false;
            }

            // Now, extract main bits.
            var pool;
            if (attributes.brief_spec) {
                pool = attributes.brief_spec.pool;
            } else {
                pool = null;
            }
            var user = attributes.authenticated_user;
            var state = attributes.state;
            var type = attributes.operation_type;

            // Map runtime progress into brief_progress (see YT-1986) if operation is in progress.
            var mapped_state = mapState(state);

            // Apply text filter.
            var text_factor = extractTextFactorForCypressItem(value, attributes);
            if (substr_filter && text_factor.indexOf(substr_filter) === -1) {
                return false;
            }

            // Apply user, state & type filters; count this operation.
            if (!register.filterAndCount(pool, user, mapped_state, type, 1)) {
                return false;
            }

            // Apply failed jobs filter.
            var has_failed_jobs =
                attributes.brief_progress &&
                attributes.brief_progress.jobs &&
                attributes.brief_progress.jobs.failed > 0;

            if (has_failed_jobs) {
                failed_jobs_count++;
            }

            if (with_failed_jobs && !has_failed_jobs) {
                return false;
            }

            if (cursor_time !== null) {
                // Check cursor position.
                if (cursor_direction === "past" && start_time >= cursor_time) {
                    return false;
                }

                if (cursor_direction === "future" && start_time <= cursor_time) {
                    return false;
                }
            }

            return true;
        });

        // Mix with archive data if we are querying all operations.
        if (!with_failed_jobs) {
            var lookup = {};
            _.each(merged_data, function(item) {
                lookup[utils.getYsonValue(item)] = true;
            });
            _.each(archive_data, function(item) {
                var value = utils.getYsonValue(item);
                var attributes = utils.getYsonAttributes(item);
                if (!lookup[value]) {
                    merged_data.push(item);
                } else {
                    // Reduce count here, because we have counted this one already
                    // while processing Cypress data.
                    var pool;
                    if (attributes.brief_spec) {
                        pool = attributes.brief_spec.pool;
                    } else {
                        pool = null;
                    }
                    register.filterAndCount(
                        pool,
                        attributes.authenticated_user,
                        attributes.state,
                        attributes.operation_type,
                        -1);
                }
            });
        }

        function startTimeComparer(direction) {
            var m;
            if (direction === "past") m = 1;
            if (direction === "future") m = -1;
            return function(a, b) {
                var aT = utils.getYsonAttribute(a, "start_time");
                var bT = utils.getYsonAttribute(b, "start_time");
                if (aT < bT) {
                    return m;
                } else if (aT > bT) {
                    return -m;
                } else {
                    return 0;
                }
            };
        }

        merged_data.sort(startTimeComparer(cursor_direction));

        // Check if there are any extra items.
        var wrap_with_incomplete = false;
        if (merged_data.length > max_size) {
            wrap_with_incomplete = true;
        }

        // Trim final result.
        merged_data = merged_data.slice(0, max_size);
        // Sort operations in descending order before producing final result.
        merged_data.sort(startTimeComparer("past"));

        if (wrap_with_incomplete) {
            merged_data = {
                $attributes: {incomplete: true},
                $value: merged_data,
            };
        }

        timings.total = new Date();

        var result = {
            operations: merged_data,
            timings: timings,
        };

        if (include_counters) {
            result.pool_counts = register.result.pool_counts;
            result.user_counts = register.result.user_counts;
            result.state_counts = register.result.state_counts;
            result.type_counts = register.result.type_counts;
            result.failed_jobs_count = failed_jobs_count;
        }

        logger.debug(
            "Fetched and filtered operations",
            {count: result.operations.length, timings: result.timings});

        return result;
    })
    .catch(function(err) {
        return Q.reject(new YtError(
            "Failed to list operations",
            YtError.ensureWrapped(err)));
    });
});

YtApplicationOperations.prototype.get = Q.method(
function YtApplicationOperations$get(parameters)
{
    var id = required(parameters, "id", validateId);

    var version = this.getVersion();

    var driver = this.driver;

    var data = version.then(function(version) {
        var id_parts = (version < 6 ? idStringToUint64 : idStringToUint64New)(id);
        var id_hi = id_parts[0];
        var id_lo = id_parts[1];

        var cypress_data = driver.executeSimple(
            "get",
            {path: "//sys/operations/" + utils.escapeYPath(id) + "/@"});

        var runtime_data = driver.executeSimple(
            "get",
            {path: "//sys/scheduler/orchid/scheduler/operations/" + utils.escapeYPath(id)});

        var archive_data = driver.executeSimple(
            "select_rows",
            {
                query: "* FROM [{}] WHERE (id_hi, id_lo) = ({}u, {}u)".format(
                    OPERATIONS_ARCHIVE_PATH,
                    id_hi.toString(10),
                    id_lo.toString(10)),
                output_format: ANNOTATED_JSON_FORMAT,
            });

        return [cypress_data, runtime_data, archive_data];
    });

    var cypress_data = data.then(function(data) { return data[0];});
    var runtime_data = data.then(function(data) { return data[1];});
    var archive_data = data.then(function(data) { return data[2];});

    return Q.settle([cypress_data, runtime_data])
    .spread(function(cypress_data, runtime_data) {
        var result = null;
        if (cypress_data.isFulfilled()) {
            result = cypress_data.value();
            if (runtime_data.isFulfilled()) {
                result.progress = _.extend(result.progress, runtime_data.value().progress);
            }
            return result;
        } else if (cypress_data.error().checkFor(500)) {
            return archive_data.then(function(result) {
                if (result.length > 0) {
                    result = tidyArchiveOperation(result[0]);
                    // TODO(sandello): Better JSON conversion here?
                    return stripJsonAnnotations(result);
                } else {
                    throw new YtError("No such operation " + id).withCode(1);
                }
            });
        } else {
            throw cypress_data.error();
        }
    })
    .catch(function(err) {
        return Q.reject(new YtError(
            "Failed to get operation details",
            err));
    });
});

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationOperations;
