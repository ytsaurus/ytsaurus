#include "program.h"
#include "bigrt.h"
#include "pipeline_executor.h"

#include <ads/bsyeti/libs/ytex/program/program.h>
#include <ads/bsyeti/libs/profiling/solomon/exporter.h>
#include <ads/bsyeti/libs/ytex/http/std_handlers.h>
#include <ads/bsyeti/libs/ytex/http/server.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <util/generic/ptr.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/server.h>

#include <memory>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

struct TBigRtProgramBase::TInternal
{
    NYT::NProfiling::TProfiler Profiler;
    NYT::NProfiling::TTagSet ProfilerTags;
    NYT::NHttp::IRequestPathMatcherPtr HttpHandlers = NYT::New<NYT::NHttp::TRequestPathMatcher>();
};

////////////////////////////////////////////////////////////////////////////////

TBigRtProgramBase::TBigRtProgramBase(NYT::TCancelableContextPtr cancelableContext, std::function<TBigRtProgramConfig&()> configGetter)
    : TProgram(std::move(cancelableContext))
    , TBigRtConfigBuilderOps(std::move(configGetter))
    , Internal_(std::make_shared<TInternal>())
{
}

NYT::NHttp::IRequestPathMatcherPtr TBigRtProgramBase::GetHttpHandlers()
{
    return Internal_->HttpHandlers;
}

void TBigRtProgramBase::Run(const TPipeline& pipeline)
{
    const TString programName = "roren";
    NYTEx::TProgramBase::StaticInitialize(GetUserConfig(), programName);
    NYT::NProfiling::TSolomonExporterPtr SolomonExporter = NBSYeti::NProfiling::CreateSolomonExporter(GetNativeConfig().GetSolomonExporterConfig());
    SolomonExporter->Start();
    //  Prepare HTTP handlers
    NYTEx::NHttp::AddStandardHandlers(
        GetHttpHandlers(),
        GetCancelableContext(),
        GetNativeConfig(),
        NBSYeti::NProfiling::GetSensorsGetter(SolomonExporter)
    );

    // Start HTTP server
    const auto threads = 1;
    NYT::NHttp::TServerConfigPtr httpConfig = NYTEx::NHttp::CreateServerConfig(GetNativeConfig().GetHttpServerConfig());
    NYT::NHttp::IServerPtr HttpServer_ = NYT::NHttp::CreateServer(httpConfig, threads);
    HttpServer_->SetPathMatcher(GetHttpHandlers());
    HttpServer_->Start();

    auto executor = ::MakeIntrusive<TBigRtPipelineExecutor>(
        GetNativeConfig(), Internal_->Profiler, Internal_->ProfilerTags, GetCancelableContext());
    executor->Run(pipeline);
}

void TBigRtProgramBase::Run(std::function<void(TPipeline&)> pipelineConstructor)
{
    auto executor = ::MakeIntrusive<TExecutorStub>();
    auto pipeline = NPrivate::MakePipeline(executor);
    pipelineConstructor(pipeline);

    Run(pipeline);
}


void TBigRtProgramBase::Run(std::function<void(TPipeline&, const TBigRtProgramConfig&)> pipelineConstructor)
{
    Run(
        [this, pipelineConstructor=std::move(pipelineConstructor)] (TPipeline& pipeline) -> void {
            InternalInvokePipelineConstructor(pipelineConstructor, pipeline);
        }
    );
}

TBigRtProgramBase& TBigRtProgramBase::SetProfiler(NYT::NProfiling::TProfiler profiler)
{
    Internal_->Profiler = profiler;
    return *this;
}

TBigRtProgramBase& TBigRtProgramBase::SetProfilerTags(NYT::NProfiling::TTagSet tagSet)
{
    Internal_->ProfilerTags = tagSet;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

