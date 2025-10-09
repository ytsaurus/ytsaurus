#pragma once

#include "public.h"

#include <yt/yql/plugin/plugin.h>

#include <yt/yt/ytlib/yql_plugin/yql_plugin_proxy.h>

#include <library/cpp/retry/retry.h>


namespace NYT::NYqlPlugin::NProcess {

////////////////////////////////////////////////////////////////////////////////

class TYqlExecutorProcess
    : public TRefCounted
    , public IYqlPlugin
{
public:
    TYqlExecutorProcess(
        int slotIndex,
        int dynamicConfigVersion,
        TYqlPluginProxy pluginProxy,
        TString unixSocketPath,
        TProcessBasePtr yqlPluginProcess,
        TFuture<void> processFinishFuture,
        TDuration runRequestTimeout);


    void Start() override;

    TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files) override;


    TQueryResult Run(
        TQueryId queryId,
        TString user,
        NYson::TYsonString credentials,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) override;


    TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId queryId,
        TString user,
        TString queryText,
        NYson::TYsonString settings,
        NYson::TYsonString credentials) override;


    TQueryResult GetProgress(TQueryId queryId) override;

    TAbortResult Abort(TQueryId queryId) override;

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) override;

    void Stop();
    void SubscribeOnFinish(TCallback<void (const TErrorOr<void>&)> callback);

    bool WaitReady();

    int DynamicConfigVersion() const;

    int SlotIndex() const;
    std::optional<TQueryId> ActiveQueryId() const;

private:
    int SlotIndex_;
    int DynamicConfigVersion_;
    TYqlPluginProxy PluginProxy_;
    TString UnixSocketPath_;
    TProcessBasePtr YqlPluginProcess_;
    TFuture<void> ProcessFinishFuture_;
    std::optional<TQueryId> ActiveQueryId_;
    TDuration RunRequestTimeout_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ActiveQueryIdLock_);

    std::shared_ptr<IRetryPolicy<const std::exception&>> StartPluginRetryPolicy_ = IRetryPolicy<const std::exception&>::GetExponentialBackoffPolicy(
        /*retryClassFunction*/ [](const std::exception&) {
            return ERetryErrorClass::LongRetry;
        },
        /*minDelay*/ TDuration::Seconds(5),
        /*minLongRetryDelay*/ TDuration::Seconds(5),
        /*maxDelay*/ TDuration::Seconds(30),
        /*maxRetries*/ 5);

private:
    template<typename T, typename R>
    T ToErrorResponse(const TFormatString<>& errorMessage, const TErrorOr<R>& response) const;

    void CheckReady();
};

DECLARE_REFCOUNTED_CLASS(TYqlExecutorProcess)
DEFINE_REFCOUNTED_TYPE(TYqlExecutorProcess)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NYqlPlugin::NProcess
