#pragma once

#include "program.h"

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/convert.h>

#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
class TProgramConfigMixin
{
protected:
    TProgramConfigMixin(NLastGetopt::TOpts& opts, bool required = true)
    {
        auto opt = opts
            .AddLongOption("config", "path to configuration file")
            .StoreMappedResult(&ConfigPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("FILE");
        if (required) {
            opt.Required();
        } else {
            opt.Optional();
        }
        opts
            .AddLongOption("config-template", "print config template and exit")
            .SetFlag(&ConfigTemplate_);
        opts
            .AddLongOption("config-actual", "print actual config and exit")
            .SetFlag(&ConfigActual_);
    }

    TIntrusivePtr<TConfig> GetConfig()
    {
        if (!Config_) {
            Load();
        }
        return Config_;
    }

    NYTree::INodePtr GetConfigNode()
    {
        if (!ConfigNode_) {
            Load();
        }
        return ConfigNode_;
    }

    bool HandleConfigOptions()
    {
        auto print = [] (const TIntrusivePtr <TConfig>& config) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
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
    void Load()
    {
        using namespace NYTree;

        try {
            TIFStream stream(ConfigPath_);
            ConfigNode_ = ConvertToNode(&stream);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing configuration file")
                << TErrorAttribute("config", ConfigPath_)
                << ex;
        }

        try {
            Config_ = New<TConfig>();
            Config_->Load(ConfigNode_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error loading configuration file")
                << TErrorAttribute("config", ConfigPath_)
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
