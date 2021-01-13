#include <random>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/misc/shutdown.h>

#include <util/generic/yexception.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

using namespace NYT;
using namespace NYT::NTracing;

int main(int argc, char* argv[])
{
    try {
        if (argc != 2 && argc != 3) {
            throw yexception() << "usage: " << argv[0] << " COLLECTOR_ENDPOINT";
        }

        auto config = New<NTracing::TJaegerTracerConfig>();
        config->CollectorChannelConfig = New<NRpc::NGrpc::TChannelConfig>();
        config->CollectorChannelConfig->Address = argv[1];

        config->FlushPeriod = TDuration::MilliSeconds(100);

        config->ServiceName = "example";
        config->ProcessTags["host"] = "prime-dev.qyp.yandex-team.ru";

        auto jaeger = New<NTracing::TJaegerTracer>(config);
        SetGlobalTracer(jaeger);

        auto traceContext = CreateRootTraceContext("Example");
        traceContext->AddTag("user", "prime");
        traceContext->SetSampled();

        Sleep(TDuration::MilliSeconds(10));
        auto childTraceContext = CreateChildTraceContext(traceContext, "Subrequest");
        childTraceContext->AddTag("index", "0");

        Sleep(TDuration::MilliSeconds(2));
        childTraceContext->Finish();

        Sleep(TDuration::MilliSeconds(2));
        traceContext->Finish();

        jaeger->WaitFlush().Get();
        Cout << ToString(traceContext->GetTraceId()) << Endl;
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        _exit(1);
    }

    NYT::Shutdown();
    return 0;
}
