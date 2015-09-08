var url = require("url");
var util = require("util");
var Q = require("bluebird");
var _ = require("underscore");

var binding = require("./ytnode");
var utils = require("./utils");

var YtError = require("./error").that;
var YtHttpRequest = require("./http_request").that;

////////////////////////////////////////////////////////////////////////////////

var __DBG = require("./debug").that("V", "Operations");

////////////////////////////////////////////////////////////////////////////////

var OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive/ordered_by_id",
    OPERATIONS_INDEX_PATH = "//sys/operations_archive/ordered_by_start_time",
    OPERATIONS_CYPRESS_PATH = "//sys/operations",
    OPERATIONS_ORCHID_PATH = "//sys/scheduler/orchid/scheduler/operations",
    SCHEDULING_INFO_PATH = "//sys/scheduler/orchid/scheduler";

var INTERMEDIATE_STATES = [
    "pending",
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

MAX_TIME_SPAN = 30

function mapState(state) {
    return INTERMEDIATE_STATES.indexOf(state) !== -1 ? "running" : state;
};

function calculateFactors(item, factors) {
    var calculated = _.map(factors, function (currentFactor) {
        return (typeof currentFactor === "function") ? currentFactor(item) : item[currentFactor];
    });

    return calculated.join(" ").toLowerCase();
}

function escape(string) {
    return "\"" + binding.EscapeC(string) + "\"";
}

function YtApplicationOperations(driver)
{
    this.list = function(parameters) {
        var text_filter = (parameters.filter || "").toString().toLowerCase();
        var user_filter = parameters.user_filter,
            current_user = parameters.current_user,
            selected_user = parameters.user,
            start_time_begin = Date.parse(parameters.start_time_begin);

        var filtered_user = user_filter === "personal" ? current_user : user_filter === "other" && selected_user;

        var state_filter = parameters.state;
        var type_filter = parameters.type;
        var jobs_filter = parameters.has_failed_jobs;


        var month_ago = new Date();
        month_ago.setDate(month_ago.getDate() - MAX_TIME_SPAN);

        if (isNaN(start_time_begin) || start_time_begin < month_ago) {
            start_time_begin = month_ago;
        }

        var date_filter = "start_time > {}".format(escape(start_time_begin.toISOString()));

        var runtime_data = driver.executeSimple(
            "get", {
                path: OPERATIONS_ORCHID_PATH
            });

        var cypress_data = driver.executeSimple(
            "list",
            {
                path: OPERATIONS_CYPRESS_PATH, 
                attributes: OPERATION_ATTRIBUTES
            });

        var text_filter = "is_substr({}, filter_factors)".format(escape(text_filter));

        var strict_filter = [text_filter, date_filter];

        var filter_condition = strict_filter.slice();

        if (filtered_user) {
            filter_condition.push("authenticated_user = {}".format(escape(filtered_user)));
        }

        if (state_filter && state_filter !== "all") {
            filter_condition.push("state = {}".format(escape(state_filter)));
        }

        if (type_filter && type_filter !== "all") {
            filter_condition.push("operation_type = {}".format(escape(type_filter)));
        }

        var source = "from [{}] join [{}] using id, id_hash, start_time"
            .format(OPERATIONS_INDEX_PATH, OPERATIONS_ARCHIVE_PATH);

        var archive_counts = driver.executeSimple(
            "select_rows",
            {   
                query: "user, state, type, sum(1) as count" +
                    " {} where {}".format(source, strict_filter.join(" and ")) +
                    " group by authenticated_user as user, state, operation_type as type"
            });

        var archive_data = driver.executeSimple(
            "select_rows",
            {
                query: "* {} where {}".format(source, filter_condition.join(" and ")) +
                    " order by finish_time desc" +
                    " limit 100"
            });

        function getFilterFactors(item) {
            return calculateFactors(item, [
                "$value",
                function (item) {
                    return calculateFactors(item.$attributes, [
                        "key",
                        "authenticated_user",
                        "state",
                        "operation_type",
                        "pool",
                        function (attributes) {
                            return attributes.brief_spec.title;
                        },
                        function (attributes) {
                            return utils.getYsonValue(
                                utils.getYsonValue(
                                    attributes.brief_spec.input_table_paths
                                )[0]
                            );
                        },
                        function (attributes) {
                            return utils.getYsonValue(
                                utils.getYsonValue(
                                    attributes.brief_spec.output_table_paths
                                )[0]
                            );
                        }
                    ]);
                },
            ]);
        }

        function makePreparer() {
            function makeCounts(names) {
                return _.reduce(names, function (counts, name) {
                    counts[name] = 0;
                    return counts;
                }, { });
            }

            var user_counts = makeCounts(["all", "personal", "other"]),
                state_counts = makeCounts(["all", "running", "completed", "failed", "aborted"]),
                type_counts = makeCounts(["all", "map", "map_reduce", "merge", "reduce", "remote_copy", "sort"]);

            var all_users = { };

            return {
                filter: function(user, state, type, count) {
                    // USER
                    if (!all_users.hasOwnProperty(user)) {
                        all_users[user] = 0;
                    }
                    all_users[user] += count; 

                    user_counts.all += count;

                    if (user === current_user) {
                        user_counts.personal += count;
                    }

                    if (!selected_user || user === selected_user) {
                        user_counts.other += count;
                    }

                    if (filtered_user && user !== filtered_user) {
                        return false;
                    }

                    // STATE
                    state_counts.all += count;
                    state_counts[state] += count;

                    if (state_filter !== "all" && state !== state_filter) {
                        return false;
                    }

                    // TYPE
                    type_counts.all += count;
                    type_counts[type] += count;

                    if (type_filter !== "all" && type !== type_filter) {
                        return false;
                    }

                    return true;
                },
                result: {
                    user: user_counts,
                    state: state_counts,
                    type: type_counts,
                    all_users: all_users
                }
            }
        }

        function mergeData(cypress_data, archive_data, archive_counts) {
            var preparer = makePreparer();

            var failed_jobs_count = 0;

            _.each(archive_counts, function (item) {
                preparer.filter(item.user, item.state, item.type, item.count);
            });

            var merged_data = _.filter(cypress_data, function (item) {
                var attributes = item.$attributes;

                // TEXT FILTER
                var text_factors = getFilterFactors(item);

                if (text_factors.indexOf(text_filter) === -1) {
                    return false;
                }

                // FILTER METHOD
                if (!preparer.filter(attributes.authenticated_user, item.state, attributes.operation_type, 1)) {
                    return false;
                }

                // FAILED JOBS
                var has_failed_jobs =
                    attributes.brief_progress &&
                    attributes.brief_progress.jobs &&
                    attributes.brief_progress.jobs.failed > 0;

                if (has_failed_jobs) {
                    failed_jobs_count++;
                }

                if (jobs_filter && !has_failed_jobs) {
                    return false;
                }

                return true;
            });

            if (!jobs_filter) {
                var operation_lookup = { };
                _.each(merged_data, function (item) {
                    operation_lookup[utils.getYsonValue(item)] = true;
                });
                _.each(archive_data, function (item) {
                    if (!operation_lookup[utils.getYsonValue(item)]) {
                        merged_data.push(item);
                    }
                });
            }

            merged_data.sort(function (a, b) {
                if (a.$attributes.start_time < b.$attributes.start_time) {
                    return 1;
                } else if (a.$attributes.start_time > b.$attributes.start_time) {
                    return -1;
                } else {
                    return 0;
                }
            });

            return {
                operations: merged_data,
                user_counts: preparer.result.user,
                state_counts: preparer.result.state,
                type_counts: preparer.result.type,
                failed_jobs_count: failed_jobs_count,
                users: preparer.result.all_users
            };
        }
 
        return Q.settle([cypress_data, runtime_data, archive_data, archive_counts])
        .spread(function (cypress_data, runtime_data, archive_data, archive_counts) {
            if (cypress_data.isRejected()) {
                return Q.reject(new YtError(
                    "Failed to get operations from Cypress",
                    cypress_data.error()));
            } else {
                cypress_data = cypress_data.value();
            }

            runtime_data = runtime_data.isRejected() ? {} : runtime_data.value();

            if (archive_data.isRejected() || archive_counts.isRejected()) {
                archive_data = [];
                archive_counts = [];
            } else {
                archive_data = archive_data.value();
                archive_counts = archive_counts.value();
            }

            archive_data = archive_data.map(function (operation) {
                return {
                    $value: operation.id,
                    $attributes: _.omit(operation, "id", "id_hash")
                };
            });
 
            cypress_data.forEach(function (item) {
                var id = utils.getYsonValue(item),
                    attributes = item.$attributes,
                    runtime_attributes;

                if (mapState(attributes.state) === "running") {
                    runtime_attributes = runtime_data[id];

                    if (runtime_attributes) {
                        // Map runtime progress into brief_progress (see YT-1986) if operation is in progress
                        utils.merge(attributes.brief_progress, runtime_attributes.progress);
                    }
                }

                item.state = mapState(attributes.state);
            });

            return mergeData(cypress_data, archive_data, archive_counts);
        })
        .catch(function(err) {
            return Q.reject(new YtError(
                "Failed to get list of operations",
                err));
        });
    };

    this.get = function (parameters) {
        var id = parameters.id;
        
        var cypress_data = driver.executeSimple(
            "get", {
                path: "//sys/operations/" + id + "/@"
            });

        var archive_data = driver.executeSimple(
            "select_rows",
            {   
                query: "* from [{}] where id = {}".format(OPERATIONS_ARCHIVE_PATH, escape(id))
            });

        return Q.settle([cypress_data, archive_data])
        .spread(function (cypress_data, archive_data) {
            if (cypress_data.isFulfilled()) {
                return cypress_data.value();
            } else if (cypress_data.error().checkFor(500)) {                
                if (archive_data.isFulfilled()) {
                    if (archive_data.value().length) {
                        return archive_data.value()[0];
                    } else {
                        throw new YtError("No such operation " + id);
                    }
                } else {
                    throw archive_data.error();
                }
            } else {
                throw cypress_data.error();
            }
        })
        .catch(function(err) {
            return Q.reject(new YtError(
                "Failed to get operation attributes",
                err));
        });
    };

    this.get_scheduling_information = function () {
        return driver.executeSimple(
            "get", {
                path: SCHEDULING_INFO_PATH
            })
        .then(function (scheduler) {
            var cell = scheduler.cell,
                pools = scheduler.pools,
                operations = scheduler.operations;

            var refinedPools = { };
            Object.keys(pools).forEach(function (id) {
                refinedPools[id] = utils.pick(pools[id], POOL_FIELDS);
            });

            return {
                cell: cell,
                pools: refinedPools, 
                operations: operations
            };
        })
        .catch(function(err) {
            return Q.reject(new YtError(
                "Failed to get scheduling information",
                err));
        });
    };
}

////////////////////////////////////////////////////////////////////////////////

exports.that = YtApplicationOperations;
