#include <yt/yt/server/lib/rpc_proxy/multiproxy_access_validator.h>
#include <yt/yt/server/lib/rpc_proxy/config.h>

#include <yt/yt/core/test_framework/framework.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NRpcProxy;

////////////////////////////////////////////////////////////////////////////////

const TMultiproxyMethodList DefaultMethodList = {
    {"enabled_method", EMultiproxyMethodKind::ExplicitlyEnabled},
    {"read_method", EMultiproxyMethodKind::Read},
    {"write_method", EMultiproxyMethodKind::Write},
    {"disabled_method", EMultiproxyMethodKind::ExplicitlyDisabled},
};

////////////////////////////////////////////////////////////////////////////////

TMultiproxyPresetDynamicConfigPtr CreatePresetConfig(
    EMultiproxyEnabledMethods enabledMethods)
{
    auto config = New<TMultiproxyPresetDynamicConfig>();
    config->EnabledMethods = enabledMethods;
    return config;
}

TMultiproxyDynamicConfigPtr CreateSinglePresetConfig(
    EMultiproxyEnabledMethods enabledMethods,
    const std::string& presetName = "default")
{
    auto config = New<TMultiproxyDynamicConfig>();
    config->Presets[presetName] = CreatePresetConfig(enabledMethods);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

class TMultiproxyAccessValidatorTest
    : public testing::Test
{
protected:
    const IMultiproxyAccessValidatorPtr Validator_;

    TMultiproxyAccessValidatorTest()
        : Validator_(NYT::NRpcProxy::CreateMultiproxyAccessValidator(DefaultMethodList))
    { }
};

#define EXPECT_DISABLED(cluster, method) EXPECT_THROW_THAT( \
    Validator_->ValidateMultiproxyAccess((cluster), (method)), \
    testing::ContainsRegex("Redirecting .* request to cluster .* is disabled by configuration"))

#define EXPECT_ENABLED(cluster, method) EXPECT_NO_THROW( \
    Validator_->ValidateMultiproxyAccess((cluster), (method)))

TEST_F(TMultiproxyAccessValidatorTest, NoConfig)
{
    EXPECT_DISABLED("cluster", "enabled_method");
    EXPECT_DISABLED("cluster", "read_method");
    EXPECT_DISABLED("cluster", "write_method");
    EXPECT_DISABLED("cluster", "disabled_method");
    EXPECT_DISABLED("cluster", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, DisableAll)
{
    Validator_->Reconfigure(CreateSinglePresetConfig(EMultiproxyEnabledMethods::DisableAll));

    EXPECT_DISABLED("cluster", "enabled_method");
    EXPECT_DISABLED("cluster", "read_method");
    EXPECT_DISABLED("cluster", "write_method");
    EXPECT_DISABLED("cluster", "disabled_method");
    EXPECT_DISABLED("cluster", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, ExplicitlyEnabled)
{
    Validator_->Reconfigure(CreateSinglePresetConfig(EMultiproxyEnabledMethods::ExplicitlyEnabled));

    EXPECT_ENABLED("cluster", "enabled_method");
    EXPECT_DISABLED("cluster", "read_method");
    EXPECT_DISABLED("cluster", "write_method");
    EXPECT_DISABLED("cluster", "disabled_method");
    EXPECT_DISABLED("cluster", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, Read)
{
    Validator_->Reconfigure(CreateSinglePresetConfig(EMultiproxyEnabledMethods::Read));

    EXPECT_ENABLED("cluster", "enabled_method");
    EXPECT_ENABLED("cluster", "read_method");
    EXPECT_DISABLED("cluster", "write_method");
    EXPECT_DISABLED("cluster", "disabled_method");
    EXPECT_DISABLED("cluster", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, Write)
{
    Validator_->Reconfigure(CreateSinglePresetConfig(EMultiproxyEnabledMethods::ReadAndWrite));

    EXPECT_ENABLED("cluster", "enabled_method");
    EXPECT_ENABLED("cluster", "read_method");
    EXPECT_ENABLED("cluster", "write_method");
    EXPECT_DISABLED("cluster", "disabled_method");
    EXPECT_DISABLED("cluster", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, OverrideMethods)
{
    auto config = CreateSinglePresetConfig(EMultiproxyEnabledMethods::Read);
    config->Presets["default"]->MethodOverrides = {
        {"read_method", EMultiproxyMethodKind::ExplicitlyDisabled},
        {"write_method", EMultiproxyMethodKind::ExplicitlyEnabled},
        {"missing_method", EMultiproxyMethodKind::ExplicitlyEnabled},
    };
    Validator_->Reconfigure(config);

    EXPECT_ENABLED("cluster", "enabled_method");
    EXPECT_DISABLED("cluster", "read_method");
    EXPECT_ENABLED("cluster", "write_method");
    EXPECT_DISABLED("cluster", "disabled_method");
    EXPECT_ENABLED("cluster", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, ExplicitCluster)
{
    auto config = New<TMultiproxyDynamicConfig>();
    config->Presets["cluster_a_preset"] = CreatePresetConfig(EMultiproxyEnabledMethods::Read);
    config->Presets["cluster_a_preset"]->MethodOverrides = {
        {"read_method", EMultiproxyMethodKind::ExplicitlyDisabled},
        {"write_method", EMultiproxyMethodKind::ExplicitlyEnabled},
        {"missing_method", EMultiproxyMethodKind::ExplicitlyEnabled},
    };
    config->ClusterPresets["cluster_a"] = "cluster_a_preset";
    Validator_->Reconfigure(config);

    EXPECT_ENABLED("cluster_a", "enabled_method");
    EXPECT_DISABLED("cluster_a", "read_method");
    EXPECT_ENABLED("cluster_a", "write_method");
    EXPECT_DISABLED("cluster_a", "disabled_method");
    EXPECT_ENABLED("cluster_a", "missing_method");

    EXPECT_DISABLED("cluster_b", "enabled_method");
    EXPECT_DISABLED("cluster_b", "read_method");
    EXPECT_DISABLED("cluster_b", "write_method");
    EXPECT_DISABLED("cluster_b", "disabled_method");
    EXPECT_DISABLED("cluster_b", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, TypoInPreset)
{
    auto config = New<TMultiproxyDynamicConfig>();
    config->Presets["cluster_a_preset"] = CreatePresetConfig(EMultiproxyEnabledMethods::ReadAndWrite);
    config->Presets["default"] = CreatePresetConfig(EMultiproxyEnabledMethods::ReadAndWrite);
    config->ClusterPresets["cluster_a"] = "typo";
    Validator_->Reconfigure(config);

    EXPECT_DISABLED("cluster_a", "enabled_method");
    EXPECT_DISABLED("cluster_a", "read_method");
    EXPECT_DISABLED("cluster_a", "write_method");
    EXPECT_DISABLED("cluster_a", "disabled_method");
    EXPECT_DISABLED("cluster_a", "missing_method");
}

TEST_F(TMultiproxyAccessValidatorTest, MultipleClusters)
{
    auto config = New<TMultiproxyDynamicConfig>();
    config->Presets["cluster_a_preset"] = CreatePresetConfig(EMultiproxyEnabledMethods::Read);
    config->Presets["cluster_a_preset"]->MethodOverrides = {
        {"read_method", EMultiproxyMethodKind::ExplicitlyDisabled},
        {"write_method", EMultiproxyMethodKind::ExplicitlyEnabled},
        {"missing_method", EMultiproxyMethodKind::ExplicitlyEnabled},
    };
    config->Presets["default"] = CreatePresetConfig(EMultiproxyEnabledMethods::ReadAndWrite);
    config->Presets["default"]->MethodOverrides = {
        {"enabled_method", EMultiproxyMethodKind::ExplicitlyDisabled},
    };
    config->ClusterPresets["cluster_a"] = "cluster_a_preset";
    Validator_->Reconfigure(config);

    EXPECT_ENABLED("cluster_a", "enabled_method");
    EXPECT_DISABLED("cluster_a", "read_method");
    EXPECT_ENABLED("cluster_a", "write_method");
    EXPECT_DISABLED("cluster_a", "disabled_method");
    EXPECT_ENABLED("cluster_a", "missing_method");

    EXPECT_DISABLED("cluster_b", "enabled_method");
    EXPECT_ENABLED("cluster_b", "read_method");
    EXPECT_ENABLED("cluster_b", "write_method");
    EXPECT_DISABLED("cluster_b", "disabled_method");
    EXPECT_DISABLED("cluster_b", "missing_method");
}

////////////////////////////////////////////////////////////////////////////////
