#pragma once

#include "program_mixin.h"

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/string/enum.h>

#include <library/cpp/yt/system/exit.h>

#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EConfigFormat,
    (Yson)
    (Json)
    (Yaml)
);

//! Derives the config format from the file name extension
//! (`.yson`, `.json`, `.yaml`/`.yml`); returns `std::nullopt` for an unknown extension.
std::optional<EConfigFormat> DeriveConfigFormatFromFileName(TStringBuf path);

//! Loads a config file at |path| and parses it, according to |format|, into a node.
NYTree::INodePtr ParseConfigNode(const std::string& path, EConfigFormat format);

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig = void>
class TProgramConfigMixin
    : public virtual TProgramMixinBase
{
protected:
    explicit TProgramConfigMixin(
        NLastGetopt::TOpts& opts,
        bool required = true,
        const std::string& argumentName = "config")
        : ArgumentName_(argumentName)
    {
        auto opt = opts
            .AddLongOption(TString(argumentName), Format("path to %v file (in YSON format)", argumentName))
            .Handler0([&] { ConfigFlag_ = true; })
            .StoreMappedResult(&ConfigPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("FILE");
        if (required) {
            opt.Required();
        } else {
            opt.Optional();
        }

        opts
            .AddLongOption(
                Format("%v-schema", argumentName),
                Format("Prints %v schema", argumentName))
            .OptionalValue(YsonSchemaFormat_, "FORMAT")
            .Handler0([&] { ConfigSchemaFlag_ = true; })
            .StoreResult(&ConfigSchema_);
        opts
            .AddLongOption(
                Format("%v-template", argumentName),
                Format("Prints %v template", argumentName))
            .OptionalArgument()
            .SetFlag(&ConfigTemplateFlag_);
        opts
            .AddLongOption(
                Format("%v-actual", argumentName),
                Format("Prints actual %v", argumentName))
            .OptionalArgument()
            .SetFlag(&ConfigActualFlag_);
        opts
            .AddLongOption(
                Format("%v-unrecognized", argumentName),
                Format("Prints unrecognized %v", argumentName))
            .OptionalArgument()
            .SetFlag(&ConfigUnrecognizedFlag_);

        opts
            .AddLongOption(
                Format("%v-unrecognized-strategy", argumentName),
                Format("Configures strategy for unrecognized attributes in %v, variants: %v",
                    argumentName,
                    JoinToString(
                        TEnumTraits<NYTree::EUnrecognizedStrategy>::GetDomainValues(),
                        [] (TStringBuilderBase* builder, NYTree::EUnrecognizedStrategy strategy) {
                            builder->AppendString(FormatEnum(strategy));
                        })))
            .DefaultValue(FormatEnum(UnrecognizedStrategy_))
            .template Handler1T<TStringBuf>([&] (TStringBuf value) {
                UnrecognizedStrategy_ = ParseEnum<NYTree::EUnrecognizedStrategy>(value);
            });

        opts
            .AddLongOption(
                Format("%v-format", argumentName),
                Format("Format of %v file, one of {%v}; derived from the file extension by default",
                    argumentName,
                    JoinToString(
                        TEnumTraits<EConfigFormat>::GetDomainValues(),
                        [] (TStringBuilderBase* builder, EConfigFormat format) {
                            builder->AppendString(FormatEnum(format));
                        })))
            .RequiredArgument("FORMAT")
            .template Handler1T<TStringBuf>([&] (TStringBuf value) {
                ConfigFormat_ = ParseEnum<EConfigFormat>(value);
            });

        if constexpr (!std::is_same_v<TDynamicConfig, void>) {
            opts
                .AddLongOption(
                    Format("dynamic-%v-schema", argumentName),
                    Format("Prints %v schema", argumentName))
                .OptionalValue(YsonSchemaFormat_, "FORMAT")
                .Handler0([&] { DynamicConfigSchemaFlag_ = true; })
                .StoreResult(&DynamicConfigSchema_);
            opts
                .AddLongOption(
                    Format("dynamic-%v-template", argumentName),
                    Format("Prints dynamic %v template", argumentName))
                .OptionalArgument()
                .SetFlag(&DynamicConfigTemplateFlag_);
        }

        RegisterMixinCallback([&] { Handle(); });
    }

    TIntrusivePtr<TConfig> GetConfig(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigFlag_) {
            return nullptr;
        }

        if (!Config_) {
            LoadConfig();
        }
        return Config_;
    }

    NYTree::INodePtr GetConfigNode(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigFlag_) {
            return nullptr;
        }

        if (!ConfigNode_) {
            LoadConfigNode();
        }
        return ConfigNode_;
    }

    const std::string& GetConfigPath() const
    {
        return ConfigPath_;
    }

private:
    const std::string ArgumentName_;

    bool ConfigFlag_;
    std::string ConfigPath_;
    std::optional<EConfigFormat> ConfigFormat_;
    bool ConfigSchemaFlag_ = false;
    std::string ConfigSchema_;
    bool ConfigTemplateFlag_;
    bool ConfigActualFlag_;
    bool ConfigUnrecognizedFlag_;
    bool DynamicConfigSchemaFlag_ = false;
    std::string DynamicConfigSchema_;
    bool DynamicConfigTemplateFlag_ = false;
    NYTree::EUnrecognizedStrategy UnrecognizedStrategy_ = NYTree::EUnrecognizedStrategy::KeepRecursive;

    static constexpr auto YsonSchemaFormat_ = "yson-schema";

    TIntrusivePtr<TConfig> Config_;
    NYTree::INodePtr ConfigNode_;

    EConfigFormat ResolveConfigFormat() const
    {
        if (ConfigFormat_) {
            return *ConfigFormat_;
        }
        // Fall back to YSON for backward compatibility with extensionless paths.
        return DeriveConfigFormatFromFileName(ConfigPath_).value_or(EConfigFormat::Yson);
    }

    void LoadConfigNode()
    {
        if (!ConfigFlag_) {
            THROW_ERROR_EXCEPTION("Missing %qv option", ArgumentName_);
        }

        try {
            ConfigNode_ = ParseConfigNode(ConfigPath_, ResolveConfigFormat());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing %v file %v",
                ArgumentName_,
                ConfigPath_)
                << ex;
        }
    }

    void LoadConfig()
    {
        if (!ConfigNode_) {
            LoadConfigNode();
        }

        try {
            Config_ = New<TConfig>();
            Config_->SetUnrecognizedStrategy(UnrecognizedStrategy_);
            Config_->Load(ConfigNode_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error loading %v file %v",
                ArgumentName_,
                ConfigPath_)
                << ex;
        }
    }

    void Handle()
    {
        auto print = [] (const auto& config) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            Cout << Endl;
        };

        auto printNode = [] (const auto& node) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            NYTree::Serialize(node, &writer);
            Cout << Endl;
        };

        auto printSchema = [] (const auto& config, const std::string& format) {
            if (format == YsonSchemaFormat_) {
                using namespace NYson;
                TYsonWriter writer(&Cout, EYsonFormat::Pretty);
                config->WriteSchema(&writer);
                Cout << Endl;
            } else {
                THROW_ERROR_EXCEPTION("Unknown schema format %Qv", format);
            }
        };

        if (ConfigSchemaFlag_) {
            printSchema(New<TConfig>(), ConfigSchema_);
            Exit(EProcessExitCode::OK);
        }

        if (ConfigTemplateFlag_) {
            print(New<TConfig>());
            Exit(EProcessExitCode::OK);
        }

        if (ConfigActualFlag_) {
            print(GetConfig());
            Exit(EProcessExitCode::OK);
        }

        if (ConfigUnrecognizedFlag_) {
            auto unrecognized = GetConfig()->GetRecursiveUnrecognized();
            printNode(*unrecognized);
            Exit(EProcessExitCode::OK);
        }

        if constexpr (!std::is_same_v<TDynamicConfig, void>) {
            if (DynamicConfigSchemaFlag_) {
                printSchema(New<TDynamicConfig>(), DynamicConfigSchema_);
                Exit(EProcessExitCode::OK);
            }

            if (DynamicConfigTemplateFlag_) {
                print(New<TDynamicConfig>());
                Exit(EProcessExitCode::OK);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
