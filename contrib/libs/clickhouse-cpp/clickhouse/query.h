#pragma once

#include "block.h"
#include "server_exception.h"

#include "base/open_telemetry.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace clickhouse {

struct QuerySettingsField {
    enum Flags : uint64_t
    {
        IMPORTANT = 0x01,
        CUSTOM = 0x02,
        OBSOLETE = 0x04,
    };
    std::string value;
    uint64_t flags{0};
};

using QuerySettings = std::unordered_map<std::string, QuerySettingsField>;
using QueryParamValue = std::optional<std::string>;
using QueryParams = std::unordered_map<std::string, QueryParamValue>;

struct Profile {
    uint64_t rows = 0;
    uint64_t blocks = 0;
    uint64_t bytes = 0;
    uint64_t rows_before_limit = 0;
    bool applied_limit = false;
    bool calculated_rows_before_limit = false;
};


struct Progress {
    uint64_t rows = 0;
    uint64_t bytes = 0;
    uint64_t total_rows = 0;
    uint64_t written_rows = 0;
    uint64_t written_bytes = 0;
};


class QueryEvents {
public:
    virtual ~QueryEvents()
    { }

    /// Some data was received.
    virtual void OnData(const Block& block) = 0;
    virtual bool OnDataCancelable(const Block& block) = 0;

    virtual void OnServerException(const Exception& e) = 0;

    virtual void OnProfile(const Profile& profile) = 0;

    virtual void OnProgress(const Progress& progress) = 0;

    /** Handle query execution logs provided by server.
     *  Amount of logs regulated by `send_logs_level` setting.
     *  By-default only `fatal` log events are sent to the client side.
     */
    virtual void OnServerLog(const Block& block) = 0;

    /// Handle query execution profile events.
    virtual void OnProfileEvents(const Block& block) = 0;

    virtual void OnFinish() = 0;
};


using ExceptionCallback        = std::function<void(const Exception& e)>;
using ProgressCallback         = std::function<void(const Progress& progress)>;
using SelectCallback           = std::function<void(const Block& block)>;
using SelectCancelableCallback = std::function<bool(const Block& block)>;
using SelectServerLogCallback  = std::function<bool(const Block& block)>;
using ProfileEventsCallback    = std::function<bool(const Block& block)>;
using ProfileCallback          = std::function<void(const Profile& profile)>;


class Query : public QueryEvents {
public:
     Query();
     Query(const char* query, const char* query_id = nullptr);
     Query(const std::string& query, const std::string& query_id = default_query_id);
    ~Query() override;

    ///
    inline const std::string& GetText() const {
        return query_;
    }

    inline const std::string& GetQueryID() const {
        return query_id_;
    }

    inline const QuerySettings& GetQuerySettings() const {
        return query_settings_;
    }

    /// Set per query settings
    inline Query& SetQuerySettings(QuerySettings query_settings) {
        query_settings_ = std::move(query_settings);
        return *this;
    }

    /// Set per query setting
    inline Query& SetSetting(const std::string& key, const QuerySettingsField& value) {
        query_settings_[key] = value;
        return *this;
    }

    inline const QueryParams& GetParams() const { return query_params_; }

    inline Query& SetParams(QueryParams query_params) {
        query_params_ = std::move(query_params);
        return *this;
    }

    inline Query& SetParam(const std::string& name, const QueryParamValue& value) {
        query_params_[name] = value;
        return *this;
    }

    inline const std::optional<open_telemetry::TracingContext>& GetTracingContext() const {
        return tracing_context_;
    }

    /// Set tracing context for open telemetry signals
    inline Query& SetTracingContext(open_telemetry::TracingContext tracing_context) {
        tracing_context_ = std::move(tracing_context);
        return *this;
    }

    /// Set handler for receiving result data.
    inline Query& OnData(SelectCallback cb) {
        select_cb_ = std::move(cb);
        return *this;
    }

    inline Query& OnDataCancelable(SelectCancelableCallback cb) {
        select_cancelable_cb_ = std::move(cb);
        return *this;
    }

    /// Set handler for receiving server's exception.
    inline Query& OnException(ExceptionCallback cb) {
        exception_cb_ = std::move(cb);
        return *this;
    }

    /// Set handler for receiving a progress of query execution.
    inline Query& OnProgress(ProgressCallback cb) {
        progress_cb_ = std::move(cb);
        return *this;
    }

    /// Set handler for receiving a server log of query exceution.
    inline Query& OnServerLog(SelectServerLogCallback cb) {
        select_server_log_cb_ = std::move(cb);
        return *this;
    }

    /// Set handler for receiving profile events.
    inline Query& OnProfileEvents(ProfileEventsCallback cb) {
        profile_events_callback_cb_ = std::move(cb);
        return *this;
    }

    inline Query& OnProfile(ProfileCallback cb) {
        profile_callback_cb_ = std::move(cb);
        return *this;
    }

    static const std::string default_query_id;

private:
    void OnData(const Block& block) override {
        if (select_cb_) {
            select_cb_(block);
        }
    }

    bool OnDataCancelable(const Block& block) override {
        if (select_cancelable_cb_) {
            return select_cancelable_cb_(block);
        } else {
            return true;
        }
    }

    void OnServerException(const Exception& e) override {
        if (exception_cb_) {
            exception_cb_(e);
        }
    }

    void OnProfile(const Profile& profile) override {
        if (profile_callback_cb_)
            profile_callback_cb_(profile);
    }

    void OnProgress(const Progress& progress) override {
        if (progress_cb_) {
            progress_cb_(progress);
        }
    }

    void OnServerLog(const Block& block) override {
        if (select_server_log_cb_) {
            select_server_log_cb_(block);
        }
    }

    void OnProfileEvents(const Block& block) override {
        if (profile_events_callback_cb_) {
            profile_events_callback_cb_(block);
        }
    }

    void OnFinish() override {
    }

private:
    std::string query_;
    std::string query_id_;
    std::optional<open_telemetry::TracingContext> tracing_context_;
    QuerySettings query_settings_;
    QueryParams query_params_;
    ExceptionCallback exception_cb_;
    ProgressCallback progress_cb_;
    SelectCallback select_cb_;
    SelectCancelableCallback select_cancelable_cb_;
    SelectServerLogCallback select_server_log_cb_;
    ProfileEventsCallback profile_events_callback_cb_;
    ProfileCallback profile_callback_cb_;
};

}
