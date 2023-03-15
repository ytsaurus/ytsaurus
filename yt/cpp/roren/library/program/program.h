#pragma once

#include <yt/cpp/roren/interface/executor.h>
#include <yt/yt/core/actions/public.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/proto_config/json_to_proto_config.h>
#include <util/stream/file.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TProgram
{
public:
    class TExecutorStub;

    TProgram(NYT::TCancelableContextPtr cancelableContext);
    ~TProgram();
    void SetCancelableContext(NYT::TCancelableContextPtr cancelableContext) = delete;
    NYT::TCancelableContextPtr GetCancelableContext() const;

    template <class TConfig>
    static TConfig LoadConfig(const TString& fileName);

    template <class TConfig>
    static TConfig LoadConfig(int argc, const char* argv[]);

protected:
    void InitSignalHandlers();

    NYT::TCancelableContextPtr CancelableContext_;
}; // class TProgram

class TProgram::TExecutorStub: public IExecutor
{
public:
    TExecutorStub() = default;
    void Run(const TPipeline& pipeline) override
    {
        Y_UNUSED(pipeline);
        std::runtime_error("TPipeline::Run() deprecated. Use TProgram::Run() instead.");
    }
}; // class TExecutorStub

template <class TConfig>
TConfig TProgram::LoadConfig(const TString& fileName)
{
    auto configText = TFileInput{fileName}.ReadAll();
    TConfig config;
    NProtobufJson::Json2Proto(std::move(configText), config);
    return config;
}

template <class TConfig>
TConfig TProgram::LoadConfig(int argc, const char* argv[])
{
    TString configFileName;
    NLastGetopt::TOpts opts;
    opts.AddLongOption("config-json", "path to json format configuration file")
        .RequiredArgument("config-file")
        .StoreResult(&configFileName)
        .Required();

    auto parsed = NLastGetopt::TOptsParseResult{&opts, argc, argv};
    return NRoren::TProgram::LoadConfig<TConfig>(configFileName);
}

void ConfigureLogs(const google::protobuf::Message& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
