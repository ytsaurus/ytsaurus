#include <yt/server/http_proxy/bootstrap.h>
#include <yt/server/http_proxy/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/json/json_parser.h>

#include <yt/core/phdr_cache/phdr_cache.h>

#include <yt/core/alloc/alloc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class THttpProxyProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NHttpProxy::TProxyConfig>
{
public:
    THttpProxyProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_, false)
    {
        Opts_
            .AddLongOption("legacy-config", "path to config in legacy format")
            .StoreMappedResult(&LegacyConfigPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("FILE")
            .Optional();
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::CurrentThreadSetName("ProxyMain");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        NYTAlloc::EnableLogging();
        NYTAlloc::EnableProfiling();
        NYTAlloc::EnableStockpile();

        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        NHttpProxy::TProxyConfigPtr config;
        NYTree::INodePtr configNode;
        if (LegacyConfigPath_) {
            TIFStream stream(LegacyConfigPath_);
            auto builder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
            builder->BeginTree();
            NJson::ParseJson(&stream, builder.get());
            configNode = NHttpProxy::ConvertFromLegacyConfig(builder->EndTree());
            config = NYTree::ConvertTo<NHttpProxy::TProxyConfigPtr>(configNode);
        } else {
            config = GetConfig();
            configNode = GetConfigNode();
        }

        ConfigureSingletons(config);


        auto* bootstrap = new NHttpProxy::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }

private:
    TString LegacyConfigPath_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::THttpProxyProgram().Run(argc, argv);
}

