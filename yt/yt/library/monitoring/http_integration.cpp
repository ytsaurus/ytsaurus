#include "http_integration.h"

#include "monitoring_manager.h"

#include <yt/yt/build/build.h>

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

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#ifdef _linux_
#include <yt/yt/library/ytprof/http/handler.h>
#include <yt/yt/library/ytprof/build_info.h>

#include <yt/yt/library/backtrace_introspector/http/handler.h>
#endif

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/string/vector.h>

namespace NYT::NMonitoring {

using namespace NYTree;
using namespace NYson;
using namespace NHttp;
using namespace NConcurrency;
using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EVerb,
    (Get)
    (List)
);

////////////////////////////////////////////////////////////////////////////////

void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterConfigPtr& config,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot)
{
    auto solomonExporter = New<NProfiling::TSolomonExporter>(config);
    Initialize(
        monitoringServer,
        solomonExporter,
        monitoringManager,
        orchidRoot);
    solomonExporter->Start();
}

void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterPtr& solomonExporter,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot)
{
    *monitoringManager = CreateMonitoringManager();
    (*monitoringManager)->Register("/ref_counted", GetCachingRefCountedTrackerStatisticsProducer());
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
        "/tcp_dispatcher",
        CreateVirtualNode(NYT::NBus::TTcpDispatcher::Get()->GetOrchidService()));

#ifdef _linux_
    auto buildInfo = NYTProf::TBuildInfo::GetDefault();
    buildInfo.BinaryVersion = GetVersion();

    SetNodeByYPath(
        *orchidRoot,
        "/build_info",
        NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("arc_revision").Value(buildInfo.ArcRevision)
                .Item("arc_last_change_num").Value(buildInfo.ArcLastChangeNum)
                .Item("binary_version").Value(buildInfo.BinaryVersion)
                .Item("build_type").Value(buildInfo.BuildType)
            .EndMap());
#endif

    if (monitoringServer) {
        if (solomonExporter) {
            solomonExporter->Register("/solomon", monitoringServer);

            SetNodeByYPath(
                *orchidRoot,
                "/sensors",
                CreateVirtualNode(solomonExporter->GetSensorService()));
        }

#ifdef _linux_
        NYTProf::Register(monitoringServer, "/ytprof", buildInfo);
        NBacktraceIntrospector::Register(monitoringServer, "/backtrace");
#endif
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

    void HandleRequest(
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

        auto verb = EVerb::Get;

        auto options = CreateEphemeralAttributes();
        for (const auto& param : params) {
            if (param.first == "verb") {
                verb = ParseEnum<EVerb>(param.second);
            } else {
                // Just a check, IAttributeDictionary takes raw YSON anyway.
                try {
                    ValidateYson(TYsonString(param.second), DefaultYsonParserNestingLevelLimit);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing value of query parameter %Qv",
                        param.first)
                        << ex;
                }

                options->SetYson(param.first, TYsonString(param.second));
            }
        }

        TYsonString result;
        switch (verb) {
            case EVerb::Get: {
                auto ypathReq = TYPathProxy::Get(path);
                ToProto(ypathReq->mutable_options(), *options);
                auto ypathRsp = WaitFor(ExecuteVerb(Service_, ypathReq))
                    .ValueOrThrow();
                result = TYsonString(ypathRsp->value());
                break;
            }
            case EVerb::List: {
                auto ypathReq = TYPathProxy::List(path);
                auto ypathRsp = WaitFor(ExecuteVerb(Service_, ypathReq))
                    .ValueOrThrow();
                result = TYsonString(ypathRsp->value());
                break;
            }
            default:
                YT_ABORT();
        }

        rsp->SetStatus(EStatusCode::OK);
        NHttp::ReplyJson(rsp, [&] (NYson::IYsonConsumer* writer) {
            Serialize(result, writer);
        });
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
