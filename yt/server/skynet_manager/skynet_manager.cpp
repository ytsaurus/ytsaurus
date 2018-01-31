#include "skynet_manager.h"

#include "private.h"

#include "bootstrap.h"
#include "skynet_api.h"

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

#include <yt/core/concurrency/async_stream.h>

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
        BIND(&TSkynetManager::Share, MakeStrong(this)));

    bootstrap->HttpServer->AddHandler("/api/v1/discover",
        BIND(&TSkynetManager::Discover, MakeStrong(this)));
}

TClusterConnectionConfigPtr TSkynetManager::GetCluster(const TString& name)
{
    for (const auto& cluster : Bootstrap_->Config->Clusters) {
        if (cluster->Name == name) {
            return cluster;
        }
    }

    THROW_ERROR_EXCEPTION("Cluster %Qv not found in config", name);
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
        LOG_ERROR(ex, "Cannon parse request parameters");

        rsp->WriteHeaders(EStatusCode::BadRequest);
        WaitFor(rsp->Close())
            .ThrowOnError();
        return;
    }

    LOG_INFO("Start creating share (Cluster: %v, Path: %v)", params.Cluster, params.Path);
    auto clusterConfig = GetCluster(params.Cluster);

    TSkynetShareMeta meta;
    std::vector<TFileOffset> fileOffsets;

    std::tie(meta, fileOffsets) = ReadSkynetMetaFromTable(
        Bootstrap_->HttpClient,
        clusterConfig->ProxyUrl,
        clusterConfig->OAuthToken,
        params.Path);

    auto skynetResource = GenerateResource(meta);
    auto callbackUrl = Format(
        "%v/api/v1/discover?cluster=%v&path=%v",
        Bootstrap_->Config->SelfUrl,
        params.Cluster,
        params.Path);

    auto asyncAddResource = Bootstrap_->SkynetApi->AddResource(
        skynetResource.RbTorrentId,
        callbackUrl,
        skynetResource.BencodedTorrentMeta);
        
    WaitFor(asyncAddResource)
        .ThrowOnError();

    LOG_INFO("Share created (RbTorrentId: %v)", skynetResource.RbTorrentId);

    rsp->GetHeaders()->Add("Content-Type", "text/plain");
    rsp->WriteHeaders(EStatusCode::Ok);
    WaitFor(rsp->WriteBody(TSharedRef::FromString(skynetResource.RbTorrentId)))
        .ThrowOnError();
}

void TSkynetManager::Discover(IRequestPtr req, IResponseWriterPtr rsp)
{
    TCgiParameters params(req->GetUrl().RawQuery);
    auto cluster = params.Get("cluster");
    TRichYPath path(params.Get("path"));

    LOG_INFO("Start serving discover (Cluster: %v, Path: %v)", cluster, path);
    auto clusterConfig = GetCluster(cluster);

    TSkynetShareMeta meta;
    std::vector<TFileOffset> fileOffsets;

    std::tie(meta, fileOffsets) = ReadSkynetMetaFromTable(
        Bootstrap_->HttpClient,
        clusterConfig->ProxyUrl,
        clusterConfig->OAuthToken,
        path);

    THttpReply reply;
    reply.Parts = FetchSkynetPartsLocations(
        Bootstrap_->HttpClient,
        clusterConfig->ProxyUrl,
        clusterConfig->OAuthToken,
        path,
        fileOffsets);

    LOG_INFO("Discover finished (Cluster: %v, Path: %v, NumberOfLocations: %v)",
        cluster,
        path,
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

std::pair<TSkynetShareMeta, std::vector<TFileOffset>> ReadSkynetMetaFromTable(
    const IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const TRichYPath& path)
{
    TSkynetShareMeta meta;
    std::vector<TFileOffset> fileOffsets;

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
    auto it = meta.Files.end();
    for (const auto& row : ConvertToNode(input)->AsList()->GetChildren()) {
        TRowMeta rowMeta;
        rowMeta.Load(row);

        if (it == meta.Files.end() || rowMeta.FileName != it->first) {
            bool ok = false;
            std::tie(it, ok) = meta.Files.emplace(rowMeta.FileName, TFileMeta{});
            if (!ok) {
                THROW_ERROR_EXCEPTION("Duplicate filenames are not allowed")
                    << TErrorAttribute("filename", rowMeta.FileName);
            }

            fileOffsets.emplace_back();
            fileOffsets.back().FilePath = rowMeta.FileName;
            fileOffsets.back().StartRow = rowIndex;
        }

        it->second.FileSize += rowMeta.DataSize;
        it->second.MD5 = MD5FromString(rowMeta.Md5);
        it->second.SHA1.emplace_back(SHA1FromString(rowMeta.Sha1));

        ++fileOffsets.back().RowCount;
        fileOffsets.back().FileSize += rowMeta.DataSize;

        ++rowIndex;
    }

    CheckTrailers(response);

    if (meta.Files.empty()) {
        THROW_ERROR_EXCEPTION("Can't share empty table");
    }

    LOG_INFO("Finished reading share meta (Path: %v, NumFiles: %v)", path, meta.Files.size());

    return {meta, fileOffsets};
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
    std::map<ui64, TString> nodeAddresses;
    for (const auto& node : nodes->GetChildren()) {
        auto nodeId = node->AsMap()->GetChild("node_id")->AsInt64()->GetValue();
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

} // namespace NSkynetManager
} // namespace NYT
