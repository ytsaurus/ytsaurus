#include "http_integration.h"

#include "monitoring_manager.h"

#include <yt/core/json/config.h>
#include <yt/core/json/json_writer.h>

#include <yt/core/yson/parser.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_detail.h>
#include <yt/core/ytree/ypath_proxy.h>

#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>
#include <yt/core/http/server.h>

#include <yt/core/ytalloc/statistics_producer.h>

#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/tracing/trace_manager.h>

#include <util/string/vector.h>
#include <library/cpp/cgiparam/cgiparam.h>

namespace NYT::NMonitoring {

using namespace NYTree;
using namespace NYson;
using namespace NHttp;
using namespace NConcurrency;
using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

struct TTraceHandlerTag {};

class TTracingHttpHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        TCgiParameters params(req->GetUrl().RawQuery);
        auto startIndex = FromString<i64>(params.Get("start_index"));
        auto limit = FromString<i64>(params.Get("limit"));

        if (auto processCheck = req->GetHeaders()->Find("X-YT-Check-Process-Id")) {
            if (*processCheck != ToString(ProcessId_)) {
                rsp->SetStatus(EStatusCode::PreconditionFailed);
                WaitFor(rsp->Close())
                    .ThrowOnError();
                return;
            }
        }

        auto [realStartIndex, traces] = NTracing::TTraceManager::Get()->ReadTraces(startIndex, limit);

        rsp->GetHeaders()->Add("X-YT-Trace-Start-Index", ToString(realStartIndex));
        rsp->GetHeaders()->Add("X-YT-Process-Id", ToString(ProcessId_));
        rsp->GetHeaders()->Add("Content-Type", "application/x-protobuf");
        rsp->SetStatus(EStatusCode::OK);

        WaitFor(rsp->WriteBody(MergeRefsToRef<TTraceHandlerTag>(traces)))
            .ThrowOnError();
    }

private:
    const TGuid ProcessId_ = TGuid::Create();
};

////////////////////////////////////////////////////////////////////////////////

class TMonitoringHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        //! The following protocol is supported:
        //! #start_sample_index parameter is expected.
        //! All samples in deque with #id more than #start_sample_index are returned (or empty vector if none).
        //! Also #index of the first corresponding sample is returned, or #start_sample_index if none).
        std::optional<i64> startSample;
        TCgiParameters params(req->GetUrl().RawQuery);
        if (auto sample = FromString<i64>(params.Get("start_sample_index"))) {
            startSample = sample;
        }
        auto [index, msg] = NProfiling::TProfileManager::Get()->GetSamples(startSample);
        rsp->GetHeaders()->Add("X-YT-Response-Start-Index", ToString(index));
        rsp->GetHeaders()->Add("X-YT-Process-Id", ToString(ProcessId_));
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(SerializeProtoToRef(msg)))
            .ThrowOnError();
        WaitFor(rsp->Close())
            .ThrowOnError();
    }

private:
    const TGuid ProcessId_ = TGuid::Create();
};

DECLARE_REFCOUNTED_CLASS(TMonitoringHandler)
DEFINE_REFCOUNTED_TYPE(TMonitoringHandler)

////////////////////////////////////////////////////////////////////////////////

void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    TMonitoringManagerPtr* manager,
    NYTree::IMapNodePtr* orchidRoot)
{
    *manager = New<TMonitoringManager>();
    (*manager)->Register("/yt_alloc", NYTAlloc::CreateStatisticsProducer());
    (*manager)->Register("/ref_counted", CreateRefCountedTrackerStatisticsProducer());
    (*manager)->Start();

    *orchidRoot = NYTree::GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        *orchidRoot,
        "/monitoring",
        CreateVirtualNode((*manager)->GetService()));
    SetNodeByYPath(
        *orchidRoot,
        "/profiling",
        CreateVirtualNode(NProfiling::TProfileManager::Get()->GetService()));

    if (monitoringServer) {
        monitoringServer->AddHandler(
            "/orchid/",
            GetOrchidYPathHttpHandler(*orchidRoot));

        monitoringServer->AddHandler(
            "/tracing/traces/v2",
            New<TTracingHttpHandler>());

        monitoringServer->AddHandler(
            "/profiling/proto",
            New<TMonitoringHandler>());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYPathHttpHandler
    : public IHttpHandler
{
public:
    TYPathHttpHandler(const IYPathServicePtr& service)
        : Service_(service)
    { }

    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        const auto orchidPrefix = AsStringBuf("/orchid");

        TString path{req->GetUrl().Path};
        if (!path.StartsWith(orchidPrefix)) {
            THROW_ERROR_EXCEPTION("HTTP request must start with %Qv prefix",
                orchidPrefix)
                << TErrorAttribute("path", path);
        }

        path = path.substr(orchidPrefix.size(), TString::npos);
        TCgiParameters params(req->GetUrl().RawQuery);

        auto ypathReq = TYPathProxy::Get(path);
        if (params.size() != 0) {
            auto options = CreateEphemeralAttributes();
            for (const auto& param : params) {
                // Just a check, IAttributeDictionary takes raw YSON anyway.
                try {
                    TYsonString(param.second).Validate();
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing value of query parameter %Qv",
                        param.first)
                        << ex;
                }

                options->SetYson(param.first, TYsonString(param.second));
                ToProto(ypathReq->mutable_options(), *options);
            }
        }

        auto ypathRsp = WaitFor(ExecuteVerb(Service_, ypathReq))
            .ValueOrThrow();

        rsp->SetStatus(EStatusCode::OK);

        auto syncOutput = CreateBufferedSyncAdapter(rsp);
        auto writer = CreateJsonConsumer(syncOutput.get());

        Serialize(TYsonString(ypathRsp->value()), writer.get());

        writer->Flush();
        syncOutput->Flush();

        WaitFor(rsp->Close())
            .ThrowOnError();
    }

private:
    IYPathServicePtr Service_;
};

IHttpHandlerPtr GetOrchidYPathHttpHandler(const IYPathServicePtr& service)
{
    return WrapYTException(New<TYPathHttpHandler>(service));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring
