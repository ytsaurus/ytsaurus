#pragma once

#include "program.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_serializable.h>

#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr char DefaultArgumentName[] = "config";

template <class TConfig, const char* ArgumentName = DefaultArgumentName>
class TProgramConfigMixin
{
protected:
    explicit TProgramConfigMixin(NLastGetopt::TOpts& opts, bool required = true)
    {
        auto opt = opts
            .AddLongOption(TString(ArgumentName), Format("path to %v file", ArgumentName))
            .StoreMappedResult(&ConfigPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("FILE");
        if (required) {
            opt.Required();
        } else {
            opt.Optional();
        }
        opts
            .AddLongOption(
                Format("%v-template", ArgumentName),
                Format("print %v template and exit", ArgumentName))
            .SetFlag(&ConfigTemplate_);
        opts
            .AddLongOption(
                Format("%v-actual", ArgumentName),
                Format("print actual %v and exit", ArgumentName))
            .SetFlag(&ConfigActual_);
    }

    TIntrusivePtr<TConfig> GetConfig(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigPath_) {
            return nullptr;
        }

        if (!Config_) {
            LoadConfig();
        }
        return Config_;
    }

    NYTree::INodePtr GetConfigNode(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigPath_) {
            return nullptr;
        }

        if (!ConfigNode_) {
            LoadConfigNode();
        }
        return ConfigNode_;
    }

    bool HandleConfigOptions()
    {
        auto print = [] (const TIntrusivePtr<TConfig>& config) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            Cout << Flush;
        };
        if (ConfigTemplate_) {
            print(New<TConfig>());
            return true;
        }
        if (ConfigActual_) {
            print(GetConfig());
            return true;
        }
        return false;
    }

private:
    void LoadConfigNode()
    {
        using namespace NYTree;

        if (!ConfigPath_){
            THROW_ERROR_EXCEPTION("Missing --%v option", ArgumentName);
        }

        try {
            TIFStream stream(ConfigPath_);
            ConfigNode_ = ConvertToNode(&stream);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing %v file %v",
                ArgumentName,
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
            Config_->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
            Config_->Load(ConfigNode_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error loading %v file %v",
                ArgumentName,
                ConfigPath_)
                << ex;
        }
    }

    TString ConfigPath_;
    bool ConfigTemplate_;
    bool ConfigActual_;

    TIntrusivePtr<TConfig> Config_;
    NYTree::INodePtr ConfigNode_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
