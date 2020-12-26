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

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig>
TDynamicConfigManagerBase<TConfig>::TDynamicConfigManagerBase(
    TDynamicConfigManagerOptions options,
    TDynamicConfigManagerConfigPtr config,
    NApi::NNative::IClientPtr masterClient,
    IInvokerPtr invoker)
    : Options_(std::move(options))
    , Config_(std::move(config))
    , MasterClient_(std::move(masterClient))
    , Invoker_(std::move(invoker))
    , UpdateExecutor_(New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TDynamicConfigManagerBase<TConfig>::DoUpdateConfig, MakeWeak(this)),
        Config_->UpdatePeriod))
    , Logger(NLogging::TLogger(DynamicConfigLogger)
        .AddTag("DynamicConfigManagerName: %v", Options_.Name))
{ }

template <typename TConfig>
void TDynamicConfigManagerBase<TConfig>::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting Dynamic Config Manager (ConfigPath: %v, UpdatePeriod: %v)",
        Options_.ConfigPath,
        Config_->UpdatePeriod);

    UpdateExecutor_->Start();
}

template <typename TConfig>
std::vector<TError> TDynamicConfigManagerBase<TConfig>::GetErrors() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TError> errors;
    auto guard = Guard(SpinLock_);
    if (!UpdateError_.IsOK()) {
        errors.push_back(UpdateError_);
    }
    if (!UnrecognizedOptionError_.IsOK()) {
        errors.push_back(UnrecognizedOptionError_);
    }

    return errors;
}

template <typename TConfig>
NYTree::IYPathServicePtr TDynamicConfigManagerBase<TConfig>::GetOrchidService() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto producer = BIND(&TDynamicConfigManagerBase<TConfig>::DoBuildOrchid, MakeStrong(this));
    return NYTree::IYPathService::FromProducer(producer);
}

template <typename TConfig>
bool TDynamicConfigManagerBase<TConfig>::IsConfigLoaded() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ConfigLoadedPromise_.IsSet();
}

template <typename TConfig>
NYTree::IMapNodePtr TDynamicConfigManagerBase<TConfig>::GetConfigNode() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return AppliedConfigNode_;
}

template <typename TConfig>
auto TDynamicConfigManagerBase<TConfig>::GetConfig() const -> TConfigPtr
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(SpinLock_);
    return AppliedConfig_;
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

    TError error;
    try {
        if (TryUpdateConfig()) {
            YT_LOG_INFO("Successfully updated dynamic config");
        } else {
            YT_LOG_DEBUG("Dynamic config was not updated");
        }
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to update dynamic config");
        error = ex;
    }

    {
        auto guard = Guard(SpinLock_);
        std::swap(UpdateError_, error);
    }
}

template <typename TConfig>
bool TDynamicConfigManagerBase<TConfig>::TryUpdateConfig()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    NApi::TGetNodeOptions getOptions;
    getOptions.ReadFrom = Options_.ReadFrom;
    auto configOrError = NConcurrency::WaitFor(MasterClient_->GetNode(Options_.ConfigPath, getOptions));
    if (configOrError.FindMatching(NYTree::EErrorCode::ResolveError) && Config_->IgnoreConfigAbsence) {
        YT_LOG_INFO("Dynamic config node does not exist (ConfigPath: %v)",
            Options_.ConfigPath);
        return false;
    }

    THROW_ERROR_EXCEPTION_IF_FAILED(configOrError,
        EErrorCode::FailedToFetchDynamicConfig,
        "Failed to fetch dynamic config from Cypress (DynamicConfigName: %v)",
        Options_.Name);

    auto configNode = NYTree::ConvertTo<NYTree::IMapNodePtr>(configOrError.Value());

    NYTree::IMapNodePtr matchedConfigNode;
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
            if (configNode->GetType() != NYTree::ENodeType::Map) {
                THROW_ERROR_EXCEPTION(
                    NDynamicConfig::EErrorCode::InvalidDynamicConfig,
                    "Dynamic config child %Qv has invalid type: expected %Qlv, actual %Qlv",
                    configFilter,
                    NYTree::ENodeType::Map,
                    configNode->GetType())
                    << TErrorAttribute("dynamic_config_name", Options_.Name);
            }

            auto configMapNode = configNode->AsMap();

            if (MakeBooleanFormula(configFilter).IsSatisfiedBy(InstanceTags_)) {
                if (matchedConfigNode) {
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::DuplicateMatchingDynamicConfigs,
                        "Found duplicate matching dynamic config")
                        << TErrorAttribute("dynamic_config_name", Options_.Name)
                        << TErrorAttribute("first_config_filter", matchingConfigFilter)
                        << TErrorAttribute("second_config_filter", configFilter);
                }

                YT_LOG_DEBUG("Found matching dynamic config (ConfigFilter: %v)",
                    configFilter);

                matchedConfigNode = configMapNode;
                matchingConfigFilter = configFilter;
            }
        }

        if (!matchedConfigNode) {
            if (Config_->IgnoreConfigAbsence) {
                return false;
            }

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
    newConfig->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
    try {
        newConfig->Load(matchedConfigNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            NDynamicConfig::EErrorCode::InvalidDynamicConfig,
            "Invalid dynamic config")
            << TErrorAttribute("dynamic_config_name", Options_.Name)
            << ex;
    }

    auto unrecognizedOptions = newConfig->GetUnrecognizedRecursively();

    TError unrecognizedOptionsError;
    if (unrecognizedOptions && unrecognizedOptions->GetChildCount() > 0 && Config_->EnableUnrecognizedOptionsAlert) {
        unrecognizedOptionsError = TError(NDynamicConfig::EErrorCode::UnrecognizedDynamicConfigOption,
            "Found unrecognized options in dynamic config (DynamicConfigName: %v)",
            Options_.Name)
            << TErrorAttribute("unrecognized_options", ConvertToYsonString(unrecognizedOptions, NYson::EYsonFormat::Text));
        YT_LOG_WARNING(unrecognizedOptionsError);
    }

    {
        auto guard = Guard(SpinLock_);
        std::swap(UnrecognizedOptionError_, unrecognizedOptionsError);
    }

    // NB: The handler could raise an exception.
    // The config must only be considered applied _after_ a successful call.
    ConfigUpdated_.Fire(AppliedConfig_, newConfig);

    {
        auto guard = Guard(SpinLock_);
        std::swap(AppliedConfigNode_, matchedConfigNode);
        std::swap(AppliedConfig_, newConfig);
        LastConfigUpdateTime_ = TInstant::Now();
    }

    ConfigLoadedPromise_.TrySet();

    return true;
}

template <typename TConfig>
void TDynamicConfigManagerBase<TConfig>::DoBuildOrchid(NYson::IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    NYTree::INodePtr configNode;
    TInstant lastConfigUpdateTime;
    {
        auto guard = Guard(SpinLock_);
        configNode = AppliedConfigNode_;
        lastConfigUpdateTime = LastConfigUpdateTime_;
    }

    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(configNode.operator bool(), [&] (auto fluent) {
                fluent.Item("applied_config").Value(configNode);
            })
            .Item("last_config_update_time").Value(lastConfigUpdateTime)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig
