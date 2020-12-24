#pragma once

#ifndef DYNAMIC_CONFIG_MANAGER_INL_H
#error "Direct inclusion of this file is not allowed, include dynamic_config_manager.h"
// For the sake of sane code completion.
#include "dynamic_config_manager.h"
#endif

#include "config.h"
#include "private.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/arithmetic_formula.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_service.h>

namespace NYT::NDynamicConfig {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig>
TDynamicConfigManagerBase<TConfig>::TDynamicConfigManagerBase(
    TDynamicConfigManagerOptions options,
    TDynamicConfigManagerConfigPtr config,
    IClientPtr masterClient,
    IInvokerPtr invoker)
    : Options_(std::move(options))
    , Config_(std::move(config))
    , MasterClient_(std::move(masterClient))
    , Invoker_(invoker)
    , UpdateExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TDynamicConfigManagerBase<TConfig>::DoUpdateConfig, MakeWeak(this)),
        Config_->UpdatePeriod))
    , Logger(TLogger{DynamicConfigLogger}
        .AddTag("DynamicConfigManagerName: %v", Options_.Name))
{ }

template <typename TConfig>
void TDynamicConfigManagerBase<TConfig>::Start()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Starting dynamic config manager (ConfigPath: %v, UpdatePeriod: %v)",
        Options_.ConfigPath,
        Config_->UpdatePeriod);

    UpdateExecutor_->Start();
}

template <typename TConfig>
std::vector<TError> TDynamicConfigManagerBase<TConfig>::GetErrors() const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    std::vector<TError> errors;
    if (!Error_.IsOK()) {
        errors.push_back(Error_);
    }
    if (!UnrecognizedOptionError_.IsOK()) {
        errors.push_back(UnrecognizedOptionError_);
    }

    return errors;
}

template <typename TConfig>
IYPathServicePtr TDynamicConfigManagerBase<TConfig>::GetOrchidService() const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto producer = BIND(&TDynamicConfigManagerBase<TConfig>::DoBuildOrchid, MakeStrong(this));
    return IYPathService::FromProducer(producer);
}

template <typename TConfig>
bool TDynamicConfigManagerBase<TConfig>::IsConfigLoaded() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ConfigLoadedPromise_.IsSet();
}

template <typename TConfig>
TFuture<void> TDynamicConfigManagerBase<TConfig>::GetConfigLoadedFuture() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ConfigLoadedPromise_.ToFuture();
}

template <typename TConfig>
std::vector<TString> TDynamicConfigManagerBase<TConfig>::GetInstanceTags() const
{
    YT_UNIMPLEMENTED();
}

template <typename TConfig>
void TDynamicConfigManagerBase<TConfig>::DoUpdateConfig()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Updating dynamic config");

    bool configUpdated = false;
    try {
        configUpdated = TryUpdateConfig();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to update dynamic config");
        Error_ = ex;
        return;
    }

    Error_ = TError();

    if (configUpdated) {
        YT_LOG_INFO("Successfully updated dynamic config");
    } else {
        YT_LOG_DEBUG("Dynamic config was not updated");
    }
}

template <typename TConfig>
bool TDynamicConfigManagerBase<TConfig>::TryUpdateConfig()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    NApi::TGetNodeOptions getOptions;
    getOptions.ReadFrom = Options_.ReadFrom;
    auto configOrError = WaitFor(MasterClient_->GetNode(Options_.ConfigPath, getOptions));
    if (configOrError.FindMatching(NYTree::EErrorCode::ResolveError) && Config_->IgnoreConfigAbsence) {
        YT_LOG_INFO("Dynamic config node does not exist (ConfigPath: %v)",
            Options_.ConfigPath);
        return false;
    }

    THROW_ERROR_EXCEPTION_IF_FAILED(configOrError,
        EErrorCode::FailedToFetchDynamicConfig,
        "Failed to fetch dynamic config from Cypress (DynamicConfigName: %v)",
        Options_.Name);

    auto configNode = ConvertTo<IMapNodePtr>(configOrError.Value());

    INodePtr matchedConfigNode;
    if (Options_.ConfigIsTagged) {
        auto instanceTags = GetInstanceTags();
        if (instanceTags != InstanceTags_) {
            YT_LOG_INFO("Instance tags list has changed (OldTagList: %v, NewTagList: %v)",
                InstanceTags_,
                instanceTags);
            InstanceTags_ = instanceTags;
        }

        auto configs = configNode->GetChildren();

        TString matchingConfigFilter;
        for (const auto& [configFilter, configNode] : configs) {
            if (MakeBooleanFormula(configFilter).IsSatisfiedBy(InstanceTags_)) {
                if (matchedConfigNode) {
                    THROW_ERROR_EXCEPTION(EErrorCode::DuplicateMatchingDynamicConfigs,
                        "Found duplicate matching dynamic config")
                        << TErrorAttribute("dynamic_config_name", Options_.Name)
                        << TErrorAttribute("first_config_filter", matchingConfigFilter)
                        << TErrorAttribute("second_config_filter", configFilter);
                }

                YT_LOG_DEBUG("Found matching dynamic config (ConfigFilter: %v)", configFilter);
                matchedConfigNode = configNode;
                matchingConfigFilter = configFilter;
            }
        }

        if (!matchedConfigNode) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuitableDynamicConfig,
                "No suitable dynamic config was found")
                << TErrorAttribute("dynamic_config_name", Options_.Name);
        }
    } else {
        matchedConfigNode = configNode;
    }

    if (AreNodesEqual(matchedConfigNode, AppliedConfigNode_)) {
        return false;
    }

    auto newConfig = New<TConfig>();
    newConfig->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    try {
        newConfig->Load(matchedConfigNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidDynamicConfig, "Invalid dynamic config")
            << TErrorAttribute("dynamic_config_name", Options_.Name)
            << ex;
    }

    auto unrecognizedOptions = newConfig->GetUnrecognizedRecursively();
    if (unrecognizedOptions && unrecognizedOptions->GetChildCount() > 0 && Config_->EnableUnrecognizedOptionsAlert) {
        auto error = TError(EErrorCode::UnrecognizedDynamicConfigOption,
            "Found unrecognized options in dynamic config (DynamicConfigName: %v)",
            Options_.Name)
            << TErrorAttribute("unrecognized_options", ConvertToYsonString(unrecognizedOptions, EYsonFormat::Text));
        YT_LOG_WARNING(error);
        UnrecognizedOptionError_ = error;
    } else {
        UnrecognizedOptionError_ = TError();
    }

    ConfigUpdated_.Fire(AppliedConfig_, newConfig);
    AppliedConfigNode_ = matchedConfigNode;
    AppliedConfig_ = newConfig;
    LastConfigUpdateTime_ = TInstant::Now();
    ConfigLoadedPromise_.TrySet();

    return true;
}

template <typename TConfig>
void TDynamicConfigManagerBase<TConfig>::DoBuildOrchid(IYsonConsumer* consumer) const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(AppliedConfigNode_), [&] (TFluentMap fluent) {
                fluent.Item("applied_config").Value(AppliedConfigNode_);
            })
            .Item("last_config_update_time").Value(LastConfigUpdateTime_)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig
