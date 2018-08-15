#include "http.h"

#include "private.h"

#include <yt/client/api/client.h>

#include <yt/client/ypath/rich.h>

#include <yt/ytlib/table_client/value_consumer.h>
#include <yt/ytlib/table_client/table_consumer.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/config.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/string.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>
#include <yt/core/http/client.h>

namespace NYT {
namespace NSkynetManager {

using namespace NHttp;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NTableClient;
using namespace NChunkClient;

static const auto& Logger = SkynetManagerLogger;

////////////////////////////////////////////////////////////////////////////////

IResponsePtr HandleRedirectAndCheckStatus(
    const IClientPtr& httpClient,
    const TString& url,
    const THeadersPtr& headers)
{
    auto rsp = WaitFor(httpClient->Get(url, headers))
        .ValueOrThrow();
    if (rsp->GetStatusCode() == EStatusCode::TemporaryRedirect) {
        rsp = WaitFor(httpClient->Get(rsp->GetHeaders()->GetOrThrow("Location"), headers))
            .ValueOrThrow();
    }

    // TODO(prime): propagate error properly.
    if (rsp->GetStatusCode() != EStatusCode::OK && rsp->GetStatusCode() != EStatusCode::Accepted) {
        THROW_ERROR_EXCEPTION("Proxy request failed with code %v", rsp->GetStatusCode())
            << ParseYTError(rsp);
    }

    return rsp;
}

void CheckTrailers(const IResponsePtr& rsp)
{
    if (rsp->GetStatusCode() == EStatusCode::Accepted) {
        if (rsp->GetTrailers()->GetOrThrow("X-Yt-Response-Code") != "0") {
            THROW_ERROR_EXCEPTION("Error in trailers")
                << ParseYTError(rsp, true);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSkynetTableValueConsumer
    : public IValueConsumer
{
public:
    TSkynetTableValueConsumer(
        i64 startRowIndex,
        const std::vector<TString>& keyColumns,
        const TProgressCallback& progressCallback)
        : NameTable_(New<TNameTable>())
        , RowIndex_(startRowIndex)
        , ProgressCallback_(progressCallback)
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

            return TString(value.Data.String, value.Length);
        };

        auto valueToInt = [&] (auto columnName) {
            if (value.Type != EValueType::Int64) {
                THROW_ERROR_EXCEPTION("Invalid column type")
                    << TErrorAttribute("column", columnName);
            }

            return value.Data.Int64;
        };
    
        if (value.Id == FilenameId_) {
            Filename_ = valueToString("filename");
        } else if (value.Id == Sha1Id_) {
            Sha1_ = valueToString("sha1");
        } else if (value.Id == Md5Id_) {
            Md5_ = valueToString("md5");
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
        auto key = KeyBuilder_.FinishRow();
        if (!Filename_ || !DataSize_ || !Md5_ || !Sha1_ || key.GetCount() != KeyColumns_.size()) {
            THROW_ERROR_EXCEPTION("Missing columns");
        }

        // Key changed    
        if (Shards_.empty() || Shards_.back().Key != key) {
            Shards_.emplace_back();
            Shards_.back().Key = key;
            CurrentFile_.Reset();
        }

        // File changed
        if (!CurrentFile_ || *CurrentFile_ != *Filename_) {
            CurrentFile_ = Filename_;

            auto file = Shards_.back().Resource.add_files();
            file->set_filename(*Filename_);
            file->set_start_row(RowIndex_);
            file->set_file_size(0);
        }

        auto files = Shards_.back().Resource.mutable_files();
        auto file = files->Mutable(files->size() - 1);

        file->set_file_size(file->file_size() + *DataSize_);
        file->set_row_count(file->row_count() + 1);
        *(file->mutable_sha1sum()) += *Sha1_;
        file->set_md5sum(*Md5_);
        
        if (ProgressCallback_) {
            ProgressCallback_(RowIndex_);
        }
        RowIndex_++;
    }

    std::vector<TTableShard> Finish()
    {
        return std::move(Shards_);
    }

    i64 GetRowIndex() const
    {
        return RowIndex_;
    }

private:
    TNameTablePtr NameTable_;
    i64 RowIndex_ = 0;
    TProgressCallback ProgressCallback_;

    std::vector<int> KeyColumns_;
    int FilenameId_;
    int Sha1Id_;
    int Md5Id_;
    int DataSizeId_;

    TNullable<TString> Filename_;
    TNullable<i64> DataSize_;
    TNullable<TString> Md5_;
    TNullable<TString> Sha1_;

    TUnversionedOwningRowBuilder KeyBuilder_;

    TNullable<TString> CurrentFile_;

    std::vector<TTableShard> Shards_;
};

struct TRequestParams
    : public TYsonSerializableLite
{
public:
    TRichYPath Path;
    TString OutputFormat;

    TRequestParams()
    {
        RegisterParameter("path", Path);
        RegisterParameter("output_format", OutputFormat)
            .Default("yson");
    }
};

std::vector<TTableShard> ReadSkynetMetaFromTable(
    const IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const TYPath& path,
    const std::vector<TString>& keyColumns,
    const TProgressCallback& progressCallback)
{
    TRequestParams params;
    params.Path = path;

    auto columns = keyColumns;
    columns.push_back("filename");
    columns.push_back("data_size");
    columns.push_back("md5");
    columns.push_back("sha1");
    params.Path.SetColumns(columns);

    auto headers = New<THeaders>();
    headers->Add("X-Yt-Header-Format", "<format=text>yson");
    headers->Add("X-Yt-Parameters", ConvertToYsonString(params, EYsonFormat::Text).GetData());
    headers->Add("Authorization", "OAuth " + oauthToken);

    auto response = HandleRedirectAndCheckStatus(httpClient, proxyUrl + "/api/v3/read_table", headers);
    TYsonInput input(response, EYsonType::ListFragment);

    auto responseAttributes = ConvertToNode(TYsonString(response->GetHeaders()->GetOrThrow("X-Yt-Response-Parameters")));
    auto startRowIndex = responseAttributes->AsMap()->GetChild("start_row_index")->AsInt64()->GetValue();

    TSkynetTableValueConsumer consumer{startRowIndex, keyColumns, progressCallback};
    TTableConsumer tableConsumer{&consumer};
    ParseYson(input, &tableConsumer);

    CheckTrailers(response);

    auto shards = consumer.Finish();
    if (shards.size() == 0) {
        THROW_ERROR_EXCEPTION("Can't share empty table");
    }
    LOG_INFO("Finished reading share meta (Path: %v, ShardsCount: %d, StartRow: %d, EndRow: %d)",
        path,
        shards.size(),
        startRowIndex,
        consumer.GetRowIndex()); 
    return shards;
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkSpec
    : public TYsonSerializableLite
{
    TGuid ChunkId;
    ui64 RangeIndex;
    ui64 RowIndex;
    ui64 RowCount;
    TReadLimit LowerLimit;
    TReadLimit UpperLimit;
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

std::vector<TRowRangeLocation> FetchSkynetPartsLocations(
    const IClientPtr& httpClient,
    const TString& proxyUrl,
    const TString& oauthToken,
    const TYPath& path)
{
    TRequestParams params;
    params.Path = path;

    std::vector<TRowRangeLocation> locations;

    auto headers = New<THeaders>();
    headers->Add("X-Yt-Header-Format", "<format=text>yson");
    headers->Add("X-Yt-Parameters", ConvertToYsonString(params, EYsonFormat::Text).GetData());
    headers->Add("Authorization", "OAuth " + oauthToken);

    auto response = HandleRedirectAndCheckStatus(httpClient, proxyUrl + "/api/v3/locate_skynet_share", headers);
    TYsonInput input(response, EYsonType::Node);
    auto ysonResponse = ConvertToNode(input);
    CheckTrailers(response);

    auto nodes = ysonResponse->AsMap()->GetChild("nodes")->AsList();
    THashMap<i64, TString> nodeAddresses;
    for (const auto& node : nodes->GetChildren()) {
        auto nodeNode = node->AsMap()->GetChild("node_id");
        i64 nodeId = ConvertTo<i64>(nodeNode);
        auto address = node->AsMap()->GetChild("addresses")->AsMap()->GetChild("default")->AsString()->GetValue();

        nodeAddresses[nodeId] = address;
    }

    auto chunks = ysonResponse->AsMap()->GetChild("chunk_specs")->AsList();

    auto specs = chunks->GetChildren();
    for (int i = 0; i < specs.size(); ++i) {
        TChunkSpec spec;
        spec.Load(specs[i]);

        TRowRangeLocation location;
        location.ChunkId = spec.ChunkId;
        location.RowIndex = spec.RowIndex;
        location.RowCount = spec.RowCount;
        if (spec.LowerLimit.HasRowIndex()) {
            location.LowerLimit = spec.LowerLimit.GetRowIndex();
        }
        for (auto nodeId : spec.Replicas) {
            location.Nodes.push_back(nodeAddresses.at(nodeId));
        }
        
        locations.push_back(location);
    }

    return locations;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
