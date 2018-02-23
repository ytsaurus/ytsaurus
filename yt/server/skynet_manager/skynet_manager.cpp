#include "skynet_manager.h"

#include "private.h"

#include "bootstrap.h"
#include "skynet_api.h"
#include "cypress_sync.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/config.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/string.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/http/client.h>
#include <yt/core/http/server.h>
#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <util/string/cgiparam.h>

namespace NYT {
namespace NSkynetManager {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NHttp;

static auto& Logger = SkynetManagerLogger;

////////////////////////////////////////////////////////////////////////////////

class TShareParameters
    : public TYsonSerializableLite
{
public:
    TString Cluster;
    TRichYPath Path;

    TShareParameters()
    {
        RegisterParameter("cluster", Cluster);
        RegisterParameter("path", Path);
    }
};

////////////////////////////////////////////////////////////////////////////////

TSkynetManager::TSkynetManager(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    bootstrap->HttpServer->AddHandler("/api/v1/share",
        WrapYTException(New<TCallbackHandler>(BIND(&TSkynetManager::Share, MakeStrong(this)))));

    bootstrap->HttpServer->AddHandler("/api/v1/discover",
        WrapYTException(New<TCallbackHandler>(BIND(&TSkynetManager::Discover, MakeStrong(this)))));

    bootstrap->HttpServer->AddHandler("/debug/healthcheck",
        BIND(&TSkynetManager::HealthCheck, MakeStrong(this)));
}

void TSkynetManager::HealthCheck(IRequestPtr req, IResponseWriterPtr rsp)
{
    bool ok = false;
    {
        auto guard = Guard(Lock_);
        ok = CypressPullErrors_.empty()
            && TablesScanErrors_.empty()
            && SyncedClusters_.size() == Bootstrap_->Clusters.size();
    }

    rsp->WriteHeaders(ok ? EStatusCode::Ok : EStatusCode::InternalServerError);
    WaitFor(rsp->Close())
        .ThrowOnError();
}

void TSkynetManager::Share(IRequestPtr req, IResponseWriterPtr rsp)
{
    if (req->GetMethod() != EMethod::Post) {
        rsp->WriteHeaders(EStatusCode::MethodNotAllowed);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    TShareParameters params;
    try {
        params.Load(ConvertToNode(TYsonString(req->GetHeaders()->Get("X-Yt-Parameters"))));
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to parse request parameters");

        rsp->WriteHeaders(EStatusCode::BadRequest);
        FillYTErrorHeaders(rsp, TError(ex));
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    LOG_INFO("Start creating share (Cluster: %v, Path: %v)", params.Cluster, params.Path);
    auto cluster = Bootstrap_->GetCluster(params.Cluster);

    auto tableRevision = CheckTableAttributes(params.Cluster, params.Path);
    if (!tableRevision.IsOK()) {
        rsp->WriteHeaders(EStatusCode::BadRequest);
        FillYTErrorHeaders(rsp, tableRevision);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    TShareKey shareKey = {params.Cluster, ToString(params.Path), tableRevision.ValueOrThrow()};

    Bootstrap_->ShareCache->CheckTombstone(shareKey)
        .ThrowOnError();
    
    auto maybeRbTorrentId = Bootstrap_->ShareCache->TryShare(shareKey, true);
    if (!maybeRbTorrentId) {
        rsp->WriteHeaders(EStatusCode::Accepted);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    auto rbTorrentId = *maybeRbTorrentId;
    LOG_INFO("Share created (Key: %v, RbTorrentId: %v)", FormatShareKey(shareKey), rbTorrentId);

    rsp->GetHeaders()->Add("Content-Type", "text/plain");
    rsp->WriteHeaders(EStatusCode::Ok);
    WaitFor(rsp->WriteBody(TSharedRef::FromString(rbTorrentId)))
        .ThrowOnError();
}

void TSkynetManager::Discover(IRequestPtr req, IResponseWriterPtr rsp)
{
    TCgiParameters params(req->GetUrl().RawQuery);
    auto rbTorrentId = params.Get("rb_torrent_id");

    LOG_INFO("Start serving discover (RbTorrentId: %v)", rbTorrentId);
    auto discoverInfo = Bootstrap_->ShareCache->TryDiscover(rbTorrentId);
    if (!discoverInfo) {
        LOG_INFO("Discover failed, share not found (RbTorrentId: %v)", rbTorrentId);
        rsp->WriteHeaders(EStatusCode::BadRequest);
        FillYTErrorHeaders(rsp, TError("Share information is missing")
            << TErrorAttribute("rb_torrent_id", rbTorrentId));
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    auto cluster = Bootstrap_->GetCluster(discoverInfo->Cluster);
    if (!cluster.UserRequestThrottler->TryAcquire(1)) {
        THROW_ERROR_EXCEPTION("User request rate limit exceeded")
            << TErrorAttribute("cluster", discoverInfo->Cluster)
            << TErrorAttribute("rbtorrent_id", rbTorrentId);
    }

    THttpReply reply;
    reply.Parts = FetchSkynetPartsLocations(
        Bootstrap_->HttpClient,
        cluster.Config->ProxyUrl,
        cluster.Config->OAuthToken,
        discoverInfo->TablePath,
        *discoverInfo->Offsets);

    LOG_INFO("Discover finished (RbTorrentId: %v, Cluster: %Qv, Path: %Qv, NumberOfLocations: %v)",
        rbTorrentId,
        discoverInfo->Cluster,
        discoverInfo->TablePath,
        reply.Parts.size());

    rsp->WriteHeaders(EStatusCode::Ok);
    auto output = CreateBufferedSyncAdapter(rsp);
    auto json = NJson::CreateJsonConsumer(output.get());

    Serialize(reply, json.get());

    json->Flush();
    output->Finish();
    WaitFor(rsp->Close())
        .ThrowOnError();
}

TString TSkynetManager::FormatDiscoveryUrl(const TString& rbTorrentId)
{
    return Format(
        "%v/api/v1/discover?rb_torrent_id=%v",
        Bootstrap_->Config->SelfUrl,
        rbTorrentId);
}

IInvokerPtr TSkynetManager::GetInvoker()
{
    return Bootstrap_->SkynetApiActionQueue->GetInvoker();
}

std::pair<TSkynetShareMeta, std::vector<TFileOffset>> TSkynetManager::ReadMeta(
    const TString& clusterName,
    const TRichYPath& path)
{
    auto& cluster = Bootstrap_->GetCluster(clusterName);
    return ReadSkynetMetaFromTable(
        Bootstrap_->HttpClient,
        cluster.Config->ProxyUrl,
        cluster.Config->OAuthToken,
        path);
}

TErrorOr<i64> TSkynetManager::CheckTableAttributes(const TString& clusterName, const TRichYPath& path)
{
    auto revision = Bootstrap_->GetCluster(clusterName).CypressSync->CheckTableAttributes(path);
    if (!revision.IsOK()) {
        LOG_ERROR(revision, "Table attributes check failed (Cluster: %Qv, Path: %Qv)", clusterName, path);
    }
    return revision;
}

void TSkynetManager::AddShareToCypress(
    const TString& clusterName,
    const TString& rbTorrentId,
    const TRichYPath& tablePath,
    i64 tableRevision)
{
    Bootstrap_->GetCluster(clusterName).CypressSync->AddShare(rbTorrentId, tablePath, tableRevision);
}

void TSkynetManager::RemoveShareFromCypress(const TString& clusterName, const TString& rbTorrentId)
{
    Bootstrap_->GetCluster(clusterName).CypressSync->RemoveShare(rbTorrentId);
}

void TSkynetManager::AddResourceToSkynet(const TString& rbTorrentId, const TString& rbTorrent)
{
    WaitFor(Bootstrap_->SkynetApi->AddResource(
        rbTorrentId,
        FormatDiscoveryUrl(rbTorrentId),
        rbTorrent))
        .ThrowOnError();
}

void TSkynetManager::RemoveResourceFromSkynet(const TString& rbTorrentId)
{
    WaitFor(Bootstrap_->SkynetApi->RemoveResource(rbTorrentId))
        .ThrowOnError();
}

void TSkynetManager::RunCypressSyncIteration(const TCluster& cluster)
{
    try {
        auto& shareCache = Bootstrap_->ShareCache;

        cluster.ThrottleBackground();
        for (const auto& shardName : cluster.CypressSync->FindChangedShards()) {
            cluster.ThrottleBackground();
            auto inCache = shareCache->ListActiveShares(shardName.first, cluster.Config->ClusterName);
            auto inCypress = cluster.CypressSync->ListShard(shardName.first, shardName.second);

            std::set<TShareKey> cachedShares(inCache.begin(), inCache.end());
            for (const auto& cypressItem : inCypress) {
                TShareKey key{cluster.Config->ClusterName, cypressItem.TablePath, cypressItem.TableRevision};

                if (cachedShares.find(key) != cachedShares.end()) {
                    cachedShares.erase(key);
                    continue;
                }

                cluster.ThrottleBackground();
                auto revision = CheckTableAttributes(std::get<0>(key), std::get<1>(key));
                if (revision.IsOK() && revision.ValueOrThrow() == cypressItem.TableRevision) {
                    shareCache->TryShare(key, false);
                } else {
                    cluster.CypressSync->RemoveShare(cypressItem.RbTorrentId);
                }
            }

            for (const auto& oldShare : cachedShares) {
                shareCache->Unshare(oldShare);
            }

            cluster.CypressSync->CommitLastSeenRevision(shardName.first, shardName.second);
        }

        auto guard = Guard(Lock_);
        CypressPullErrors_.erase(cluster.Config->ClusterName);
        SyncedClusters_.insert(cluster.Config->ClusterName);
    } catch (const std::exception& ex) {
        auto guard = Guard(Lock_);
        CypressPullErrors_[cluster.Config->ClusterName] = TError(ex);
        throw;
    }
}

void TSkynetManager::RunTableScanIteration(const TCluster& cluster)
{
    auto& shareCache = Bootstrap_->ShareCache;
    try {
        cluster.ThrottleBackground();
        for (const auto& shardName : shareCache->ListShards()) {
            cluster.ThrottleBackground();
            for (const auto& shareKey : shareCache->ListActiveShares(shardName, cluster.Config->ClusterName)) {
                cluster.ThrottleBackground();
                auto currentRevision = CheckTableAttributes(std::get<0>(shareKey), std::get<1>(shareKey));
                if (!currentRevision.IsOK() || std::get<2>(shareKey) != currentRevision.ValueOrThrow()) {
                    shareCache->Unshare(shareKey);
                }
            }
        }

        auto guard = Guard(Lock_);
        TablesScanErrors_.erase(cluster.Config->ClusterName);
    } catch (const std::exception& ex) {
        auto guard = Guard(Lock_);
        TablesScanErrors_[cluster.Config->ClusterName] = TError(ex);
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const THttpPartLocation& partLocation, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("filename").Value(partLocation.Filename)
            .Item("range").BeginList()
                .Item().Value(partLocation.RangeStart)
                .Item().Value(partLocation.RangeEnd)
            .EndList()
            .Item("locations").Value(partLocation.Locations)
        .EndMap();
}

void Serialize(const THttpReply& reply, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("parts").Value(reply.Parts)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestParams
    : public TYsonSerializableLite
{
public:
    TRichYPath Path;
    INodePtr OutputFormat;

    TRequestParams()
    {
        RegisterParameter("path", Path);
        RegisterParameter("output_format", OutputFormat)
            .Default(ConvertToNode(TYsonString("yson")));
    }
};

struct TRowMeta
    : public TYsonSerializableLite
{
    TString FileName;
    int64_t DataSize;

    TString Md5;
    TString Sha1;

    TRowMeta()
    {
        RegisterParameter("filename", FileName);
        RegisterParameter("data_size", DataSize);
        RegisterParameter("md5", Md5);
        RegisterParameter("sha1", Sha1);
    }
};

IResponsePtr HandleRedirectAndCheckStatus(
    const IClientPtr& httpClient,
    const TString& url,
    const THeadersPtr& headers)
{
    auto rsp = WaitFor(httpClient->Get(url, headers))
        .ValueOrThrow();
    if (rsp->GetStatusCode() == EStatusCode::TemporaryRedirect) {
        rsp = WaitFor(httpClient->Get(rsp->GetHeaders()->Get("Location"), headers))
            .ValueOrThrow();
    }

    if (rsp->GetStatusCode() != EStatusCode::Ok && rsp->GetStatusCode() != EStatusCode::Accepted) {
        THROW_ERROR_EXCEPTION("Proxy request failed with code %v",
            rsp->GetStatusCode());
    }

    return rsp;
}

void CheckTrailers(const IResponsePtr& rsp)
{
    if (rsp->GetStatusCode() == EStatusCode::Accepted) {
        if (rsp->GetTrailers()->Get("X-Yt-Response-Code") != "0") {
            THROW_ERROR_EXCEPTION("Error in response")
                << TErrorAttribute("message", rsp->GetTrailers()->Get("X-Yt-Response-Message"));
        }
    }
}

DEFINE_ENUM(ESkynetTableColumn,
    (NoColumn)
    (Filename)
    (DataSize)
    (Sha1)
    (Md5)
);

class TSkynetTableConsumer
    : public IYsonConsumer
{
public:
    TSkynetTableConsumer(i64 startRowIndex)
        : Meta_{}
        , RowIndex_(startRowIndex)
        , CurrentFile_{Meta_.Files.end()}
    { }

    virtual void OnStringScalar(const TStringBuf& value) override
    {
        if (Column_ == ESkynetTableColumn::Filename) {
            Filename_ = TString(value);
        } else if (Column_ == ESkynetTableColumn::Sha1) {
            Sha1_ = SHA1FromString(value);
        } else if (Column_ == ESkynetTableColumn::Md5) {
            Md5_ = MD5FromString(value);
        }
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        if (Column_ == ESkynetTableColumn::DataSize) {
            DataSize_ = value;
        }
    }

    virtual void OnUint64Scalar(ui64 value) override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnDoubleScalar(double value) override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnBooleanScalar(bool value) override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnEntity() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnBeginList() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnListItem() override
    {
        Column_ = ESkynetTableColumn::NoColumn;
        Filename_.Reset();
        DataSize_.Reset();
        Sha1_.Reset();
        Md5_.Reset();
    }

    virtual void OnEndList() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnBeginMap() override
    { }

    virtual void OnKeyedItem(const TStringBuf& key) override
    {
        if (key == "filename") {
            Column_ = ESkynetTableColumn::Filename;
        } else if (key == "data_size") {
            Column_ = ESkynetTableColumn::DataSize;
        } else if (key == "md5") {
            Column_ = ESkynetTableColumn::Md5;
        } else if (key == "sha1") {
            Column_ = ESkynetTableColumn::Sha1;
        }
    }

    virtual void OnEndMap() override
    {
        auto checkColumn = [this] (auto column, TStringBuf name) {
            if (!column) {
                THROW_ERROR_EXCEPTION("Column is missing")
                    << TErrorAttribute("column_name", name);
            }
        };

        checkColumn(Filename_, "filename");
        checkColumn(DataSize_, "data_size");
        checkColumn(Sha1_, "sha1");
        checkColumn(Md5_, "md5");

        if (CurrentFile_ == Meta_.Files.end() || *Filename_ != CurrentFile_->first) {
            bool ok = false;
            std::tie(CurrentFile_, ok) = Meta_.Files.emplace(*Filename_, TFileMeta{});
            if (!ok) {
                THROW_ERROR_EXCEPTION("Duplicate filenames are not allowed")
                    << TErrorAttribute("filename", *Filename_);
            }

            FileOffsets_.emplace_back();
            FileOffsets_.back().FilePath = *Filename_;
            FileOffsets_.back().StartRow = RowIndex_;
        }

        CurrentFile_->second.FileSize += *DataSize_;
        CurrentFile_->second.MD5 = *Md5_;
        CurrentFile_->second.SHA1.emplace_back(*Sha1_);

        ++FileOffsets_.back().RowCount;
        FileOffsets_.back().FileSize += *DataSize_;

        ++RowIndex_;
    }

    virtual void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnEndAttributes() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual void OnRaw(const TStringBuf& yson, EYsonType type) override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    std::pair<TSkynetShareMeta, std::vector<TFileOffset>> Finish()
    {
        return {std::move(Meta_), std::move(FileOffsets_)};
    }

private:
    TSkynetShareMeta Meta_;
    std::vector<TFileOffset> FileOffsets_;

    i64 RowIndex_;
    std::map<TString, TFileMeta>::iterator CurrentFile_;
    TNullable<TString> Filename_;
    TNullable<i64> DataSize_;
    TNullable<TMD5Hash> Md5_;
    TNullable<TSHA1Hash> Sha1_;

    ESkynetTableColumn Column_ = ESkynetTableColumn::NoColumn;
};

std::pair<TSkynetShareMeta, std::vector<TFileOffset>> ReadSkynetMetaFromTable(
    const IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const TRichYPath& path)
{
    TRequestParams params;
    params.Path = path;
    params.Path.SetColumns({"filename", "data_size", "md5", "sha1"});

    auto headers = New<THeaders>();
    headers->Add("X-Yt-Header-Format", "<format=text>yson");
    headers->Add("X-Yt-Parameters", ConvertToYsonString(params, EYsonFormat::Text).GetData());
    headers->Add("Authorization", "OAuth " + oauthToken);

    auto response = HandleRedirectAndCheckStatus(httpClient, proxyUrl + "/api/v3/read_table", headers);
    TYsonInput input(response, EYsonType::ListFragment);

    auto responseAttributes = ConvertToNode(TYsonString(response->GetHeaders()->Get("X-Yt-Response-Parameters")));
    auto rowIndex = responseAttributes->AsMap()->GetChild("start_row_index")->AsInt64()->GetValue();
    TSkynetTableConsumer consumer{rowIndex};

    ParseYson(input, &consumer);

    CheckTrailers(response);

    auto meta = consumer.Finish();

    if (meta.first.Files.empty()) {
        THROW_ERROR_EXCEPTION("Can't share empty table");
    }

    LOG_INFO("Finished reading share meta (Path: %v, NumFiles: %v)", path, meta.first.Files.size());

    return meta;
}

TString MakeFileUrl(
    const TString& node,
    const TGuid& chunkId,
    ui64 lowerRowIndex,
    ui64 upperRowIndex,
    ui64 partIndex)
{
    return Format(
        "http://%v/read_skynet_part?chunk_id=%v&lower_row_index=%v&upper_row_index=%v&start_part_index=%v",
        node,
        chunkId,
        lowerRowIndex,
        upperRowIndex,
        partIndex);
}

struct TChunkSpec
    : public TYsonSerializableLite
{
    TGuid ChunkId;
    ui64 RangeIndex;
    ui64 RowIndex;
    ui64 RowCount;
    NChunkClient::TReadLimit LowerLimit;
    NChunkClient::TReadLimit UpperLimit;
    std::vector<ui64> Replicas;

    TChunkSpec()
    {
        RegisterParameter("chunk_id", ChunkId);
        RegisterParameter("range_index", RangeIndex);
        RegisterParameter("row_index", RowIndex);
        RegisterParameter("row_count", RowCount);
        RegisterParameter("lower_limit", LowerLimit)
            .Default();
        RegisterParameter("upper_limit", UpperLimit)
            .Default();
        RegisterParameter("replicas", Replicas);
    }
};

std::vector<THttpPartLocation> FetchSkynetPartsLocations(
    const IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const TRichYPath& path,
    const std::vector<TFileOffset>& fileOffsets)
{
    std::vector<THttpPartLocation> locations;

    TRequestParams params;
    params.Path = path;

    auto headers = New<THeaders>();
    headers->Add("X-Yt-Header-Format", "<format=text>yson");
    headers->Add("X-Yt-Parameters", ConvertToYsonString(params, EYsonFormat::Text).GetData());
    headers->Add("Authorization", "OAuth " + oauthToken);

    auto response = HandleRedirectAndCheckStatus(httpClient, proxyUrl + "/api/v3/locate_skynet_share", headers);
    TYsonInput input(response, EYsonType::Node);
    auto ysonResponse = ConvertToNode(input);
    CheckTrailers(response);

    auto nodes = ysonResponse->AsMap()->GetChild("nodes")->AsList();
    std::map<i64, TString> nodeAddresses;
    for (const auto& node : nodes->GetChildren()) {
        auto nodeNode = node->AsMap()->GetChild("node_id");
        i64 nodeId;
        // COMPAT(prime@)
        if (nodeNode->GetType() == ENodeType::Int64) {
            nodeId = nodeNode->AsInt64()->GetValue();
        } else {
            nodeId = nodeNode->AsUint64()->GetValue();
        }
        auto address = node->AsMap()->GetChild("addresses")->AsMap()->GetChild("default")->AsString()->GetValue();

        nodeAddresses[nodeId] = address;
    }
    
    auto chunks = ysonResponse->AsMap()->GetChild("chunk_specs")->AsList();

    // Loop is moving two iterators through table. One iterator is
    // represented by [file, fileRowIndex] pair, and the second is
    // represented by [chunkSpec, globalRowIndex] pair.

    // First iterator
    auto currentFile = fileOffsets.begin();
    ui64 currentFileRow = 0;

    auto specs = chunks->GetChildren();
    for (int i = 0; i < specs.size(); ++i) {
        TChunkSpec spec;
        spec.Load(specs[i]);

        // Second iterator
        ui64 currentRowIndex = spec.RowIndex;
        if (spec.LowerLimit.HasRowIndex()) {
            currentRowIndex += spec.LowerLimit.GetRowIndex();
        }

        if (currentFile == fileOffsets.end()) {
            THROW_ERROR_EXCEPTION("File offsets are inconsistent with chunk specs")
                << TErrorAttribute("file_index", currentFile - fileOffsets.begin())
                << TErrorAttribute("current_file_row", currentFileRow)
                << TErrorAttribute("chunk_spec", spec);
        }

        while (true) {
            if (currentRowIndex == spec.RowIndex + spec.RowCount) {
                break;
            }
            YCHECK(currentRowIndex < spec.RowIndex + spec.RowCount);

            ui64 nextRowIndex = currentRowIndex;
            nextRowIndex += std::min(
                currentFile->RowCount - currentFileRow,
                spec.RowCount - (currentRowIndex - spec.RowIndex));

            THttpPartLocation location;
            location.Filename = currentFile->FilePath;
            location.RangeStart = SkynetPieceSize * currentFileRow;
            location.RangeEnd = std::min(
                currentFile->FileSize,
                SkynetPieceSize * (currentFileRow + nextRowIndex - currentRowIndex));

            for (auto nodeId : spec.Replicas) {
                auto it = nodeAddresses.find(nodeId);
                if (it == nodeAddresses.end()) {
                    THROW_ERROR_EXCEPTION("Received unknown node_id during skynet share parts location");
                }

                location.Locations.emplace_back(MakeFileUrl(
                    it->second,
                    spec.ChunkId,
                    currentRowIndex - spec.RowIndex,
                    nextRowIndex - spec.RowIndex,
                    currentFileRow));
            }
            locations.emplace_back(std::move(location));

            currentFileRow += nextRowIndex - currentRowIndex;
            if (currentFileRow == currentFile->RowCount) {
                currentFileRow = 0;
                ++currentFile;

                if (currentFile == fileOffsets.end()) {
                    break;
                }
            }
            YCHECK(currentFileRow < currentFile->RowCount);

            currentRowIndex = nextRowIndex;
        }
    }

    return locations;
}

////////////////////////////////////////////////////////////////////////////////

void CleanSkynet(const ISkynetApiPtr& skynetApi, const TShareCachePtr& shareCache)
{
    auto resources = WaitFor(skynetApi->ListResources())
        .ValueOrThrow();

    i64 count = 0;
    for (const auto& rbTorrentId : resources) {
        if (!shareCache->TryDiscover(rbTorrentId)) {
            ++count;
            WaitFor(skynetApi->RemoveResource(rbTorrentId))
                .ThrowOnError();
        }
    }

    LOG_INFO("Removed %d unknown shares", count);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
