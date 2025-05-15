#include "multiproxy_access_validator.h"

#include "config.h"

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger = RpcProxyLogger();
static const std::string DefaultKey = "default";

////////////////////////////////////////////////////////////////////////////////

static bool CheckMethodEnabled(EMultiproxyEnabledMethods enabledMethods, EMultiproxyMethodKind methodKind)
{
    return ToUnderlying(enabledMethods) >= ToUnderlying(methodKind);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TAllowedMethods)

struct TAllowedMethods
    : public TRefCounted
{
    THashSet<std::string> AllowedMethods;

    static const TAllowedMethodsPtr& Empty()
    {
        static const auto empty = New<TAllowedMethods>();
        return empty;
    }
};

DEFINE_REFCOUNTED_TYPE(TAllowedMethods);

////////////////////////////////////////////////////////////////////////////////

class TMultiproxyAccessValidator
    : public IMultiproxyAccessValidator
{
public:
    explicit TMultiproxyAccessValidator(TMultiproxyMethodList knownMethods)
        : KnownMethods_(std::move(knownMethods))
    {
        Reconfigure(New<TMultiproxyDynamicConfig>());
    }

    void ValidateMultiproxyAccess(const std::string& cluster, const std::string& method) override
    {
        auto guard = ReaderGuard(Lock_);
        auto allowedMethodsIt = ClusterAllowedMethods_.find(cluster);
        const auto& preset = allowedMethodsIt == ClusterAllowedMethods_.end()
            ? DefaultAllowedMethods_
            : allowedMethodsIt->second;

        if (!preset->AllowedMethods.contains(method)) {
            THROW_ERROR_EXCEPTION("Redirecting %Qv request to cluster %Qv is disabled by configuration",
                method,
                cluster);
        }
    }

    void Reconfigure(const TMultiproxyDynamicConfigPtr& config) override
    {
        auto presetToAllowedMethods = [knownMethods = &KnownMethods_] (const TMultiproxyPresetDynamicConfigPtr& config) {
            auto result = New<TAllowedMethods>();
            for (const auto& [methodName, methodKind] : config->MethodOverrides) {
                if (CheckMethodEnabled(config->EnabledMethods, methodKind)) {
                    result->AllowedMethods.emplace(methodName);
                }
            }

            for (const auto& [methodName, methodKind] : *knownMethods) {
                if (config->MethodOverrides.contains(methodName)) {
                    // Already considered this method.
                    continue;
                }
                if (CheckMethodEnabled(config->EnabledMethods, methodKind)) {
                    result->AllowedMethods.emplace(methodName);
                }
            }

            return result;
        };

        THashMap<std::string, TAllowedMethodsPtr> nameToAllowedMethods;
        for (const auto& [name, presetConfig] : config->Presets) {
            nameToAllowedMethods.emplace(name, presetToAllowedMethods(presetConfig));
        }

        TAllowedMethodsPtr newDefaultAllowedMethods;
        if (auto it = nameToAllowedMethods.find(DefaultKey); it != nameToAllowedMethods.end()) {
            newDefaultAllowedMethods = std::move(it->second);
            nameToAllowedMethods.erase(it);
        } else {
            static const TAllowedMethodsPtr emptyAllowedMethods = New<TAllowedMethods>();
            newDefaultAllowedMethods = emptyAllowedMethods;
        }

        THashMap<std::string, TAllowedMethodsPtr> newClusterAllowedMethods;
        for (const auto& [clusterName, presetName] : config->ClusterPresets) {
            if (presetName == DefaultKey) {
                continue;
            }

            auto it = nameToAllowedMethods.find(presetName);
            if (it == nameToAllowedMethods.end()) {
                YT_LOG_ERROR("Multiproxy preset is not found in configuration (Preset: %Qv, Cluster: %Qv)",
                    presetName,
                    clusterName);
                newClusterAllowedMethods.emplace(clusterName, TAllowedMethods::Empty());
            } else {
                newClusterAllowedMethods.emplace(clusterName, it->second);
            }
        }

        {
            auto guard = WriterGuard(Lock_);
            std::swap(newClusterAllowedMethods, ClusterAllowedMethods_);
            std::swap(newDefaultAllowedMethods, DefaultAllowedMethods_);
        }
    }

private:
    const std::vector<std::pair<std::string, EMultiproxyMethodKind>> KnownMethods_;

    THashMap<std::string, TAllowedMethodsPtr> ClusterAllowedMethods_;
    TAllowedMethodsPtr DefaultAllowedMethods_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
};

////////////////////////////////////////////////////////////////////////////////

IMultiproxyAccessValidatorPtr CreateMultiproxyAccessValidator(TMultiproxyMethodList methodList)
{
    return New<TMultiproxyAccessValidator>(std::move(methodList));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
