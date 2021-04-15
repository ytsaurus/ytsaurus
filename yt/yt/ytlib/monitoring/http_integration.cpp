#include "http_integration.h"

#include "monitoring_manager.h"

#include <yt/yt/core/json/config.h>
#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/ytalloc/statistics_producer.h>

#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/string/vector.h>

namespace NYT::NMonitoring {

using namespace NYTree;
using namespace NYson;
using namespace NHttp;
using namespace NConcurrency;
using namespace NJson;

void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterConfigPtr& config,
    TMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot)
{
    *monitoringManager = New<TMonitoringManager>();
    (*monitoringManager)->Register("/yt_alloc", NYTAlloc::CreateStatisticsProducer());
    (*monitoringManager)->Register("/ref_counted", CreateRefCountedTrackerStatisticsProducer());
    (*monitoringManager)->Register("/solomon", BIND([] (NYson::IYsonConsumer* consumer) {
        auto tags = NProfiling::TSolomonRegistry::Get()->GetDynamicTags();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("dynamic_tags").Value(THashMap<TString, TString>(tags.begin(), tags.end()))
            .EndMap();
    }));
    (*monitoringManager)->Start();

    *orchidRoot = NYTree::GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        *orchidRoot,
        "/monitoring",
        CreateVirtualNode((*monitoringManager)->GetService()));
    SetNodeByYPath(
        *orchidRoot,
        "/profiling",
        CreateVirtualNode(NProfiling::TProfileManager::Get()->GetService()));

    if (monitoringServer) {
        auto exporter = New<NProfiling::TSolomonExporter>(
            config,
            NProfiling::TProfileManager::Get()->GetInvoker());
        exporter->Register("/solomon", monitoringServer);
        exporter->Start();

        SetNodeByYPath(
            *orchidRoot,
            "/sensors",
            CreateVirtualNode(exporter->GetSensorService()));

        monitoringServer->AddHandler(
            "/orchid/",
            GetOrchidYPathHttpHandler(*orchidRoot));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYPathHttpHandler
    : public IHttpHandler
{
public:
    explicit TYPathHttpHandler(IYPathServicePtr service)
        : Service_(std::move(service))
    { }

    virtual void HandleRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp) override
    {
        const TStringBuf orchidPrefix = "/orchid";

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
    const IYPathServicePtr Service_;
};

IHttpHandlerPtr GetOrchidYPathHttpHandler(const IYPathServicePtr& service)
{
    return WrapYTException(New<TYPathHttpHandler>(service));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring
