#include "skynet_service.h"

#include "peer_connection.h"
#include "announcer.h"
#include "private.h"
#include "bootstrap.h"
#include "http.h"

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/net/listener.h>
#include <yt/core/net/local_address.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/yson/string.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/json/json_writer.h>

#include <yt/core/http/stream.h>
#include <yt/core/http/helpers.h>
#include <yt/core/http/server.h>

#include <yt/ytlib/table_client/skynet_column_evaluator.h>

namespace NYT {
namespace NSkynetManager {

using namespace NApi;
using namespace NCypressClient;
using namespace NNet;
using namespace NHttp;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NLogging;

static const auto& Logger = SkynetManagerLogger;

////////////////////////////////////////////////////////////////////////////////

TShareOperation::TShareOperation(
    TRequestKey request,
    const TClusterConnectionPtr& cluster,
    const TAnnouncerPtr& announcer)
    : Request_(request)
    , Cluster_(cluster)
    , Announcer_(announcer)
    , Logger(SkynetManagerLogger)
{
    Logger.AddTag("Request: %v", request);
}

void TShareOperation::Start(const IInvokerPtr& invoker)
{
    BIND(&TShareOperation::Run, MakeStrong(this))
        .AsyncVia(invoker)
        .Run();
}

void TShareOperation::Run()
{
    try {
        std::vector<TResourceId> resources;
        try {
            LOG_INFO("Share operation started");
            auto lastUpdate = TInstant::Now();
            i64 lastRowIndex = 0;
            auto updateProgress = [&] (i64 rowIndex) {
                if (rowIndex < lastRowIndex + 1024 || TInstant::Now() < lastUpdate + TDuration::Seconds(10) ) {
                    return;
                }

                lastRowIndex = rowIndex;
                lastUpdate = TInstant::Now();
                Cluster_->GetTables()->UpdateStatus(
                    Request_,
                    BuildYsonStringFluently()
                        .BeginMap()
                            .Item("stage").Value("reading_table")
                            .Item("row_index").Value(rowIndex)
                        .EndMap(),
                    Null);
            };
            
            auto shards = Cluster_->ReadSkynetMetaFromTable(
                Request_.TablePath,
                Request_.KeyColumns,
                updateProgress);
            LOG_INFO("Finished reading hashes");
            resources = Cluster_->GetTables()->FinishRequest(Request_, shards);
            LOG_INFO("Resources created");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Saving error");
            Cluster_->GetTables()->UpdateStatus(Request_, Null, TError(ex));
            throw;
        }

        auto announceStart = TInstant::Now();
        std::vector<TFuture<void>> announces;
        for (auto resource : resources) {
            announces.push_back(Announcer_->AddOutOfOrderAnnounce(Cluster_->GetName(), resource));
        }
            
        WaitFor(Combine(announces))
            .ThrowOnError();
        LOG_INFO("Finished announcing (Duration: %v)", (TInstant::Now() - announceStart));
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Share operation crashed");
    }
}

////////////////////////////////////////////////////////////////////////////////

TClusterConnection::TClusterConnection(
    const TClusterConnectionConfigPtr& config,
    const NApi::IClientPtr& client,
    const NHttp::IClientPtr& httpClient)
    : Config_(config)
    , BackgroundThrottler_(CreateReconfigurableThroughputThrottler(Config_->BackgroundThrottler))
    , Client_(client)
    , HttpClient_(httpClient)
    , Tables_(New<TTables>(client, config))
{ }

TString TClusterConnection::GetName() const
{
    return Config_->ClusterName;
}

const TTablesPtr& TClusterConnection::GetTables() const
{
    return Tables_;
}

const NConcurrency::IThroughputThrottlerPtr& TClusterConnection::GetBackgroundThrottler() const
{
    return BackgroundThrottler_;
}

bool TClusterConnection::IsHealthy() const
{
    return true; // TODO(prime@):
}

std::vector<TTableShard> TClusterConnection::ReadSkynetMetaFromTable(
    const TYPath& path,
    const std::vector<TString>& keyColumns,
    const TProgressCallback& progressCallback)
{
    return NSkynetManager::ReadSkynetMetaFromTable(
        HttpClient_,
        *Config_->Connection->ClusterUrl,
        Config_->OAuthToken,
        path,
        keyColumns,
        progressCallback);
}

std::vector<TRowRangeLocation> TClusterConnection::FetchSkynetPartsLocations(
    const TYPath& path)
{
    return NSkynetManager::FetchSkynetPartsLocations(
        HttpClient_,
        *Config_->Connection->ClusterUrl,
        Config_->OAuthToken,
        path);
}

TErrorOr<i64> TClusterConnection::CheckTableAttributes(const NYPath::TRichYPath& path)
{
    TGetNodeOptions options;
    options.Attributes = {
        "type",
        "account",
        "row_count",
        "schema",
        "enable_skynet_sharing",
        "revision",
    };

    try {
        auto asyncGet = Client_->GetNode(path.Normalize().GetPath(), options);
        auto node = ConvertToNode(WaitFor(asyncGet).ValueOrThrow());
        const auto& attributes = node->Attributes();
 
        if (attributes.Get<NObjectClient::EObjectType>("type") != EObjectType::Table) {
            return TError("Cypress node is not a table");
        }

        if (attributes.Get<i64>("row_count") == 0) {
            return TError("Table is empty")
                << TErrorAttribute("table_path", path.Normalize().GetPath());
        }

        if (!attributes.Get<bool>("enable_skynet_sharing", false)) {
            return TError("\"enable_skynet_sharing\" attribute is not set");
        }

        auto schema = attributes.Get<NTableClient::TTableSchema>("schema");
        try {
            NTableClient::ValidateSkynetSchema(schema);
        } catch (const TErrorException& ex) {
            return ex.Error();
        }

        // TODO(prime): keep per-account usage statistics
        auto account = attributes.Get<TString>("account");

        return attributes.Get<i64>("revision");
    } catch (const TErrorException& ex) {
        if (ex.Error().GetCode() == NYTree::EErrorCode::ResolveError) {
            return ex.Error();
        }
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

static const TString DebugLinksEndpoint = "/debug/links/";

////////////////////////////////////////////////////////////////////////////////

TSkynetService::TSkynetService(TBootstrap* bootstrap, const TString& peerId)
    : TAsyncExpiringCache<TCacheKey, TCachedResourcePtr>(bootstrap->GetConfig()->Cache)
    , Config_(bootstrap->GetConfig())
    , Announcer_(bootstrap->GetAnnouncer())
    , PeerListener_(bootstrap->GetPeerListener())
    , Invoker_(bootstrap->GetInvoker())
    , Clusters_(bootstrap->GetClusters())
    , SelfPeerId_(peerId)
    , SelfPeerName_(GetLocalHostName())
{
    auto http = bootstrap->GetHttpServer();
    http->AddHandler(
        "/api/v1/share",
        WrapYTException(New<TCallbackHandler>(BIND(&TSkynetService::HandleShare, MakeStrong(this)))));
    http->AddHandler(
        "/debug/healthcheck",
        BIND(&TSkynetService::HandleHealthCheck, MakeStrong(this)));
    http->AddHandler(
        DebugLinksEndpoint,
        BIND(&TSkynetService::HandleDebugLinks, MakeStrong(this)));
}

void TSkynetService::Start()
{
    BIND(&TSkynetService::AcceptPeers, MakeStrong(this))
        .Via(Invoker_)
        .Run();

    for (auto&& cluster : Clusters_) {
        BIND(&TSkynetService::SyncResourcesLoop, MakeStrong(this), cluster)
            .Via(Invoker_)
            .Run();

        BIND(&TSkynetService::ReapRemovedTablesLoop, MakeStrong(this), cluster)
            .Via(Invoker_)
            .Run();
    }
}

void TSkynetService::AcceptPeers()
{
    while (true) {
        auto connection = WaitFor(PeerListener_->Accept())
            .ValueOrThrow();

        auto peerConnection = New<TPeerConnection>(connection);
        BIND(&TSkynetService::HandlePeerConnection, MakeStrong(this), peerConnection)
            .AsyncVia(Invoker_)
            .Run();
    }
}

void TSkynetService::SyncResourcesLoop(TClusterConnectionPtr cluster)
{
    while (true) {
        try {
            auto resources = cluster->GetTables()->ListResources();
            LOG_INFO("Found %d resources (Cluster: %s)", resources.size(), cluster->GetName());
            Announcer_->SyncResourceList(cluster->GetName(), std::move(resources));
            LOG_INFO("Finished syncing resources (Cluster: %s)", cluster->GetName());
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error loading resource list (Cluster: %s)", cluster->GetName());
        }

        TDelayedExecutor::WaitForDuration(Config_->SyncIterationInterval);
    }
}

void TSkynetService::ReapRemovedTablesLoop(TClusterConnectionPtr cluster)
{
    auto throttler = cluster->GetBackgroundThrottler();
    auto tables = cluster->GetTables();

    while (true) {
        try {
            auto requests = tables->ListActiveRequests();
            LOG_INFO("Found %d active requests (Cluster: %s)", requests.size(), cluster->GetName());
            for (auto&& request : requests) {
                WaitFor(throttler->Throttle(1))
                    .ThrowOnError();
            
                auto errorOrRevision = cluster->CheckTableAttributes(request.TablePath);
                if (!errorOrRevision.IsOK()) {
                    LOG_INFO("Table has been removed (RequestKey: %v)", request);
                    tables->EraseRequest(request);
                } else if (errorOrRevision.Value() != request.TableRevision) {
                    LOG_INFO("Table has been changed (RequestKey: %v, OldRevision: %d, NewRevision: %d)",
                        request.TableRevision,
                        errorOrRevision.Value());
                    tables->EraseRequest(request);
                }
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Removed tables reaper crashed (Cluster: %s)", cluster->GetName());
        }

        TDelayedExecutor::WaitForDuration(Config_->RemovedTablesScanInterval);
    }
}

class TShareParameters
    : public TYsonSerializableLite
{
public:
    TString Cluster;
    TRichYPath Path;
    std::vector<TString> KeyColumns;

    TShareParameters()
    {
        RegisterParameter("cluster", Cluster);
        RegisterParameter("path", Path);
        RegisterParameter("key_columns", KeyColumns)
            .Default();
    }
};

void TSkynetService::HandleShare(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    if (req->GetMethod() != EMethod::Post) {
        rsp->SetStatus(EStatusCode::MethodNotAllowed);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    TShareParameters params;
    try {
        params.Load(ConvertToNode(TYsonString(req->GetHeaders()->GetOrThrow("X-Yt-Parameters"))));
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to parse request parameters");

        rsp->SetStatus(EStatusCode::BadRequest);
        FillYTErrorHeaders(rsp, TError(ex));
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    LOG_INFO("Start creating share (Cluster: %v, Path: %v)", params.Cluster, params.Path);
    auto cluster = GetCluster(params.Cluster);
    auto tableRevision = cluster->CheckTableAttributes(params.Path).ValueOrThrow();

    TRequestKey request{
        ToString(params.Path),
        tableRevision,
        params.KeyColumns
    };

    TRequestState requestState;
    auto started = cluster->GetTables()->StartRequest(request, &requestState);
    if (started) {
        New<TShareOperation>(request, cluster, Announcer_)->Start(Invoker_);
    }

    WriteShareReply(rsp, request, requestState);
}

void TSkynetService::WriteShareReply(
    const NHttp::IResponseWriterPtr& rsp,
    const TRequestKey& request,
    const TRequestState& state)
{
    if (state.State == ERequestState::Creating) {
        rsp->SetStatus(EStatusCode::Accepted);
        if (state.Progress) {
            TString progressHeader;
            TStringOutput progressHeaderStream(progressHeader);
            auto json = NJson::CreateJsonConsumer(&progressHeaderStream);
            BuildYsonFluently(json.get()).Value(*state.Progress);
            json->Flush();
            progressHeaderStream.Finish();
            rsp->GetHeaders()->Add("X-YT-Progress", progressHeader);
        }
        WaitFor(rsp->Close())
            .ThrowOnError();
    } else if (state.State == ERequestState::Active) {
        YCHECK(state.Resources.HasValue());
    
        // COMPAT(prime)
        if (request.KeyColumns.size() == 0) {
            auto link = state.Resources->at(0);
            auto rbTorrentId = "rbtorrent:" + link->ResourceId;

            rsp->GetHeaders()->Add("Content-Type", "text/plain");
            rsp->SetStatus(EStatusCode::OK);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(rbTorrentId)))
                .ThrowOnError();
        } else {
            rsp->GetHeaders()->Add("Content-Type", "application/json");
            rsp->SetStatus(EStatusCode::OK);

            auto output = CreateBufferedSyncAdapter(rsp);
            auto json = NJson::CreateJsonConsumer(output.get());

            BuildYsonFluently(json.get())
                .BeginMap()
                    .Item("torrents")
                        .DoListFor(*state.Resources, [&] (auto fluent, auto&& link) {
                            fluent.Item()
                                .BeginMap()
                                    .Item("key").Value(link->Key)
                                    .Item("rbtorrent").Value("rbtorrent:" + link->ResourceId)
                                .EndMap();
                        })
                .EndMap();

            json->Flush();
            output->Finish();
            WaitFor(rsp->Close())
                .ThrowOnError();
        }
    } else if (state.State == ERequestState::Failed) {
        state.Error.Get(TError{}).ThrowOnError();
    }
}

void TSkynetService::HandleHealthCheck(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    bool ok = Announcer_->IsHealthy();
    for (auto&& cluster : Clusters_) {
        ok = ok && cluster->IsHealthy();
    }

    rsp->SetStatus(ok ? EStatusCode::OK : EStatusCode::InternalServerError);
    WaitFor(rsp->Close())
        .ThrowOnError();
}

void TSkynetService::HandleDebugLinks(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    TResourceId resourceId{req->GetUrl().Path.substr(DebugLinksEndpoint.size())};
    LOG_DEBUG("Debug links (ResourceId: %s)", resourceId);

    TString clusterName;
    try {
        clusterName = Announcer_->FindResourceCluster(resourceId);
    } catch (std::exception& ex) {
        rsp->SetStatus(EStatusCode::NotFound);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    auto clusterConnection = GetCluster(clusterName);

    TYPath tableRange;
    NProto::TResource resource;
    clusterConnection->GetTables()->GetResource(resourceId, &tableRange, &resource);

    auto locations = clusterConnection->FetchSkynetPartsLocations(tableRange);

    rsp->SetStatus(EStatusCode::OK);
    rsp->GetHeaders()->Add("Content-Type", "application/json");

    auto output = CreateBufferedSyncAdapter(rsp);
    auto json = NJson::CreateJsonConsumer(output.get());

    auto links = MakeLinks(resource, locations);

    Serialize(links, json.get());

    json->Flush();
    output->Finish();
    WaitFor(rsp->Close())
        .ThrowOnError();
}

TClusterConnectionPtr TSkynetService::GetCluster(const TString& clusterName) const
{
    for (auto&& cluster : Clusters_) {
        if (cluster->GetName() == clusterName) {
            return cluster;
        }
    }

    THROW_ERROR_EXCEPTION("Cluster not found")
        << TErrorAttribute("cluster_name", clusterName);
}

void TSkynetService::HandlePeerConnection(TPeerConnectionPtr peer)
{
    LOG_INFO("Accepted peer connection (Address: %v)", peer->PeerAddress());
    try {
        auto handshake = peer->ReceiveHandshake();
        auto resourceId = handshake.ResourceId;
        auto Logger = TLogger(SkynetManagerLogger)
            .AddTag("PeerName: %s", handshake.PeerName)
            .AddTag("ResourceId: %s", resourceId);

        LOG_INFO("Started handshake");

        THandshake reply;
        reply.PeerId = SelfPeerId_;
        reply.PeerName = SelfPeerName_;
        reply.ResourceId = handshake.ResourceId;
        peer->SendHandshake(reply);
        peer->SendPing();

        LOG_INFO("Handshake completed");
        auto clusterName = Announcer_->FindResourceCluster(resourceId);
        auto clusterConnection = GetCluster(clusterName);

        TResourceLock resourceLock(Announcer_.Get(), resourceId);

        auto cachedResource = WaitFor(Get({clusterName, resourceId}))
            .ValueOrThrow();
        LOG_DEBUG("Found resource (Cluster: %s, TableRange: %v)",
            clusterName,
            cachedResource->TableRange);

        peer->SendHasResource();

        bool peerHasResource = false;
        bool linksSent = false;
        while (true) {
            auto message = peer->ReceiveMessage();
            if (message == EPeerMessage::WantResource) {
                LOG_INFO("Sending resource");

                auto description = ConvertResource(cachedResource->Resource, false, true);
                peer->SendResource(description.TorrentMeta);
                peerHasResource = true;
            } else if (message == EPeerMessage::HasResource) {
                peerHasResource = true;
            } else if (message == EPeerMessage::Ping) {
                LOG_INFO("Sending ping");
                peer->SendPing();
            }

            if (peerHasResource && !linksSent) {
                if (!resourceLock.TryAcquire()) {
                    LOG_INFO("Skipping sending links; Resource is locked by another peer");
                    continue;
                }

                auto locations = clusterConnection->FetchSkynetPartsLocations(cachedResource->TableRange);
                LOG_INFO("Sending links");

                auto links = MakeLinks(cachedResource->Resource, locations);
                peer->SendLinks(links);
                linksSent = true;
            }
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Peer connection terminated");
        throw;
    }
}

TFuture<TCachedResourcePtr> TSkynetService::DoGet(const TCacheKey& key)
{
    return BIND([this, this_ = MakeStrong(this), key] () {
        auto clusterConnection = GetCluster(key.first);
        auto entry = New<TCachedResource>();
        clusterConnection->GetTables()->GetResource(
            key.second,
            &(entry->TableRange),
            &(entry->Resource));
        return entry;
    })
        .AsyncVia(Invoker_)
        .Run();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
