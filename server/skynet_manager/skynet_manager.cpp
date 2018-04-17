#include "skynet_manager.h"

#include "private.h"

#include "bootstrap.h"
#include "skynet_api.h"
#include "cypress_sync.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/value_consumer.h>
#include <yt/ytlib/table_client/table_consumer.h>
#include <yt/ytlib/table_client/name_table.h>

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
using namespace NChunkClient;
using namespace NTableClient;

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
    bootstrap->GetHttpServer()->AddHandler("/api/v1/share",
        WrapYTException(New<TCallbackHandler>(BIND(&TSkynetManager::Share, MakeStrong(this)))));

    bootstrap->GetHttpServer()->AddHandler("/api/v1/discover",
        WrapYTException(New<TCallbackHandler>(BIND(&TSkynetManager::Discover, MakeStrong(this)))));

    bootstrap->GetHttpServer()->AddHandler("/debug/healthcheck",
        BIND(&TSkynetManager::HealthCheck, MakeStrong(this)));
}

void TSkynetManager::HealthCheck(IRequestPtr req, IResponseWriterPtr rsp)
{
    bool ok = false;
    {
        auto guard = Guard(Lock_);
        ok = CypressPullErrors_.empty()
            && TablesScanErrors_.empty()
            && SyncedClusters_.size() == Bootstrap_->GetClustersCount();
    }

    rsp->SetStatus(ok ? EStatusCode::Ok : EStatusCode::InternalServerError);
    WaitFor(rsp->Close())
        .ThrowOnError();
}

void TSkynetManager::Share(IRequestPtr req, IResponseWriterPtr rsp)
{
    if (req->GetMethod() != EMethod::Post) {
        rsp->SetStatus(EStatusCode::MethodNotAllowed);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    TShareParameters params;
    try {
        params.Load(ConvertToNode(TYsonString(req->GetHeaders()->Get("X-Yt-Parameters"))));
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to parse request parameters");

        rsp->SetStatus(EStatusCode::BadRequest);
        FillYTErrorHeaders(rsp, TError(ex));
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    LOG_INFO("Start creating share (Cluster: %v, Path: %v)", params.Cluster, params.Path);
    auto cluster = Bootstrap_->GetCluster(params.Cluster);

    auto tableRevision = CheckTableAttributes(params.Cluster, params.Path);
    if (!tableRevision.IsOK()) {
        rsp->SetStatus(EStatusCode::BadRequest);
        FillYTErrorHeaders(rsp, tableRevision);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    TShareKey shareKey{params.Cluster, ToString(params.Path), tableRevision.ValueOrThrow()};

    Bootstrap_->GetShareCache()->CheckTombstone(shareKey)
        .ThrowOnError();

    auto maybeRbTorrentId = Bootstrap_->GetShareCache()->TryShare(shareKey, true);
    if (!maybeRbTorrentId) {
        rsp->SetStatus(EStatusCode::Accepted);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    auto rbTorrentId = *maybeRbTorrentId;
    LOG_INFO("Share created (Key: %v, RbTorrentId: %v)", FormatShareKey(shareKey), rbTorrentId);

    rsp->GetHeaders()->Add("Content-Type", "text/plain");
    rsp->SetStatus(EStatusCode::Ok);
    WaitFor(rsp->WriteBody(TSharedRef::FromString(rbTorrentId)))
        .ThrowOnError();
}

void TSkynetManager::Discover(IRequestPtr req, IResponseWriterPtr rsp)
{
    TCgiParameters params(req->GetUrl().RawQuery);
    auto rbTorrentId = params.Get("rb_torrent_id");

    LOG_INFO("Start serving discover (RbTorrentId: %v)", rbTorrentId);
    auto discoverInfo = Bootstrap_->GetShareCache()->TryDiscover(rbTorrentId);
    if (!discoverInfo) {
        LOG_INFO("Discover failed, share not found (RbTorrentId: %v)", rbTorrentId);
        rsp->SetStatus(EStatusCode::BadRequest);
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
        Bootstrap_->GetHttpClient(),
        cluster.Config->ProxyUrl,
        cluster.Config->OAuthToken,
        discoverInfo->TablePath,
        *discoverInfo->Offsets);

    LOG_INFO("Discover finished (RbTorrentId: %v, Cluster: %Qv, Path: %Qv, NumberOfLocations: %v)",
        rbTorrentId,
        discoverInfo->Cluster,
        discoverInfo->TablePath,
        reply.Parts.size());

    rsp->SetStatus(EStatusCode::Ok);
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
        Bootstrap_->GetConfig()->SelfUrl,
        rbTorrentId);
}

IInvokerPtr TSkynetManager::GetInvoker()
{
    return Bootstrap_->GetInvoker();
}

std::pair<TSkynetShareMeta, std::vector<TFileOffset>> TSkynetManager::ReadMeta(
    const TString& clusterName,
    const TRichYPath& path)
{
    auto& cluster = Bootstrap_->GetCluster(clusterName);
    return ReadSkynetMetaFromTable(
        Bootstrap_->GetHttpClient(),
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
    WaitFor(Bootstrap_->GetSkynetApi()->AddResource(
        rbTorrentId,
        FormatDiscoveryUrl(rbTorrentId),
        rbTorrent))
        .ThrowOnError();
}

void TSkynetManager::RemoveResourceFromSkynet(const TString& rbTorrentId)
{
    WaitFor(Bootstrap_->GetSkynetApi()->RemoveResource(rbTorrentId))
        .ThrowOnError();
}

void TSkynetManager::RunCypressSyncIteration(const TCluster& cluster)
{
    try {
        auto& shareCache = Bootstrap_->GetShareCache();

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
    auto& shareCache = Bootstrap_->GetShareCache();
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

class TSkynetTableValueConsumer
    : public IValueConsumer
{
public:
    TSkynetTableValueConsumer(
        i64 startRowIndex,
        const std::vector<TString>& keyColumns)
        : NameTable_(New<TNameTable>())
        , RowIndex_(startRowIndex)
    {
        for (auto keyColumnName : keyColumns) {
            KeyColumns_.push_back(NameTable_->GetIdOrRegisterName(keyColumnName));
        }
        FilenameId_ = NameTable_->GetIdOrRegisterName("filename");
        Sha1Id_ = NameTable_->GetIdOrRegisterName("sha1");
        Md5Id_ = NameTable_->GetIdOrRegisterName("md5");
        DataSizeId_ = NameTable_->GetIdOrRegisterName("data_size");
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual bool GetAllowUnknownColumns() const override
    {
        return false;
    }

    virtual void OnBeginRow() override
    {
        Filename_.Reset();
        DataSize_.Reset();
        Md5_.Reset();
        Sha1_.Reset();
    }
    
    virtual void OnValue(const TUnversionedValue& value)
    {
        auto valueToString = [&] (auto columnName) {
            if (value.Type != EValueType::String) {
                THROW_ERROR_EXCEPTION("Invalid column type")
                    << TErrorAttribute("column", columnName);
            }

            return TStringBuf(value.Data.String, value.Length);
        };

        auto valueToInt = [&] (auto columnName) {
            if (value.Type != EValueType::Int64) {
                THROW_ERROR_EXCEPTION("Invalid column type")
                    << TErrorAttribute("column", columnName);
            }

            return value.Data.Int64;
        };
    
        if (value.Id == FilenameId_) {
            Filename_ = TString(valueToString("filename"));
        } else if (value.Id == Sha1Id_) {
            Sha1_ = SHA1FromString(valueToString("sha1"));
        } else if (value.Id == Md5Id_) {
            Md5_ = MD5FromString(valueToString("md5"));
        } else if (value.Id == DataSizeId_) {
            DataSize_ = valueToInt("data_size");
        } else {
            if (!std::binary_search(KeyColumns_.begin(), KeyColumns_.end(), value.Id)) {
                THROW_ERROR_EXCEPTION("Unknown column")
                    << TErrorAttribute("id", value.Id);
            }

            KeyBuilder_.AddValue(value);
        }
    }

    virtual void OnEndRow()
    {
        // Key changed
        auto key = KeyBuilder_.FinishRow();
        if (!Filename_ || !DataSize_ || !Md5_ || !Sha1_ || key.GetCount() != KeyColumns_.size()) {
            THROW_ERROR_EXCEPTION("Missing columns");
        }
    
        if (Shards_.empty() || Shards_.back().Key != key) {
            Shards_.emplace_back();
            Shards_.back().Key = key;
            CurrentFile_.Reset();
        }

        // File changed
        if (!CurrentFile_ || *CurrentFile_ != *Filename_) {
            CurrentFile_ = Filename_;

            auto& offsets = Shards_.back().Offsets;
            offsets.emplace_back();
            offsets.back().FilePath = *Filename_;
            offsets.back().StartRow = RowIndex_;
        }

        auto& file = Shards_.back().Meta.Files[*CurrentFile_];
        
        file.FileSize += *DataSize_;
        file.MD5 = *Md5_;
        file.SHA1.emplace_back(*Sha1_);

        Shards_.back().Offsets.back().RowCount++;
        Shards_.back().Offsets.back().FileSize += *DataSize_;
        
        RowIndex_++;
    }

    std::vector<TTableShard> Finish()
    {
        return std::move(Shards_);
    }

private:
    TNameTablePtr NameTable_;
    i64 RowIndex_ = 0;

    std::vector<int> KeyColumns_;
    int FilenameId_;
    int Sha1Id_;
    int Md5Id_;
    int DataSizeId_;

    TNullable<TString> Filename_;
    TNullable<i64> DataSize_;
    TNullable<TMD5Hash> Md5_;
    TNullable<TSHA1Hash> Sha1_;

    TUnversionedOwningRowBuilder KeyBuilder_;

    TNullable<TString> CurrentFile_;

    std::vector<TTableShard> Shards_;
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

    TSkynetTableValueConsumer consumer{rowIndex, {}};
    TTableConsumer tableConsumer{&consumer};
    ParseYson(input, &tableConsumer);

    CheckTrailers(response);

    auto shards = consumer.Finish();
    if (shards.size() != 1) {
        THROW_ERROR_EXCEPTION("Can't share empty table");
    }
    auto meta = shards[0].Meta;

    if (meta.Files.empty()) {
        THROW_ERROR_EXCEPTION("Can't share empty table");
    }

    LOG_INFO("Finished reading share meta (Path: %v, NumFiles: %v)", path, meta.Files.size());
    return {shards[0].Meta, shards[0].Offsets};
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
    params.Path = path.Normalize();

    if (fileOffsets.empty()) {
        THROW_ERROR_EXCEPTION("Empty offsets");
    }
    params.Path.SetRanges({
        TReadRange{
            TReadLimit().SetRowIndex(fileOffsets[0].StartRow),
            TReadLimit().SetRowIndex(fileOffsets.back().StartRow + fileOffsets.back().RowCount)
        }
    });

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
            spec.RowCount += spec.LowerLimit.GetRowIndex();
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

            ui64 nextRowIndex = currentRowIndex;
            nextRowIndex += std::min(
                currentFile->RowCount - currentFileRow,
                spec.RowCount - (currentRowIndex - spec.RowIndex));
            YCHECK(nextRowIndex != currentRowIndex);

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
