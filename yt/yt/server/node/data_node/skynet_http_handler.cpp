#include "skynet_http_handler.h"

#include "bootstrap.h"
#include "local_chunk_reader.h"
#include "chunk_meta_manager.h"

#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/chunk.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/api/native/table_reader.h>

#include <yt/yt/client/table_client/blob_reader.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>

#include <library/cpp/cgiparam/cgiparam.h>

namespace NYT::NDataNode {

using namespace NHttp;
using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NClusterNode;
using namespace NConcurrency;

using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("SkynetHandler");

////////////////////////////////////////////////////////////////////////////////

void ParseRequest(TStringBuf rawQuery, TChunkId* chunkId, TReadRange* readRange, i64* partIndex)
{
    TCgiParameters params(rawQuery);

    if (!params.Has("chunk_id")) {
        THROW_ERROR_EXCEPTION("Missing parameter \"chunk_id\" in URL query string.");
    }

    *chunkId = TChunkId::FromString(params.Get("chunk_id"));

    if (!params.Has("lower_row_index")) {
        THROW_ERROR_EXCEPTION("Missing parameter \"lower_row_index\" in URL query string.");
    }
    if (!params.Has("upper_row_index")) {
        THROW_ERROR_EXCEPTION("Missing parameter \"upper_row_index\" in URL query string.");
    }

    readRange->LowerLimit().SetRowIndex(FromString<i64>(params.Get("lower_row_index")));
    readRange->UpperLimit().SetRowIndex(FromString<i64>(params.Get("upper_row_index")));

    if (!params.Has("start_part_index")) {
        THROW_ERROR_EXCEPTION("Missing parameter \"start_part_index\" in URL query string.");
    }

    *partIndex = FromString<i64>(params.Get("start_part_index"));

    if (*partIndex < 0 || readRange->LowerLimit().GetRowIndex() < 0 || readRange->UpperLimit().GetRowIndex() < 0) {
        THROW_ERROR_EXCEPTION("Parameter is negative")
            << TErrorAttribute("part_index", *partIndex)
            << TErrorAttribute("read_range", *readRange);
    }
}

void AdjustReadRange(
    std::pair<i64, i64> httpRange,
    TReadRange* readRange,
    i64* startPartIndex,
    i64* skipPrefix,
    std::optional<i64>* byteLimit)
{
    constexpr i64 SkynetPartSize = 4_MB;

    httpRange.second += 1;
    if (httpRange.first >= httpRange.second) {
        THROW_ERROR_EXCEPTION("Invalid http range")
            << TErrorAttribute("http_range", httpRange);
    }

    auto skipRows = httpRange.first / SkynetPartSize;
    auto newLowerRowIndex = *readRange->LowerLimit().GetRowIndex() + skipRows;
    if (newLowerRowIndex >= *readRange->UpperLimit().GetRowIndex()) {
        THROW_ERROR_EXCEPTION("HTTP range start is invalid");
    }

    *startPartIndex += skipRows;
    readRange->LowerLimit().SetRowIndex(newLowerRowIndex);

    *skipPrefix = httpRange.first - skipRows * SkynetPartSize;
    *byteLimit = httpRange.second - httpRange.first;

    auto fullRows = (byteLimit->value() + *skipPrefix + SkynetPartSize - 1) / SkynetPartSize;
    auto lastRow = newLowerRowIndex + fullRows;
    if (lastRow > *readRange->UpperLimit().GetRowIndex()) {
        THROW_ERROR_EXCEPTION("HTTP range end is invalid");
    }
    readRange->UpperLimit().SetRowIndex(lastRow);
}

////////////////////////////////////////////////////////////////////////////////

class TSkynetHttpHandler
    : public IHttpHandler
{
public:
    explicit TSkynetHttpHandler(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        try {
            DoHandleRequest(req, rsp);
        } catch (const std::exception& ex) {
            if (rsp->AreHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }

private:
    void DoHandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
    {
        TChunkId chunkId;
        TReadRange readRange;
        i64 startPartIndex;

        ParseRequest(req->GetUrl().RawQuery, &chunkId, &readRange, &startPartIndex);

        std::optional<i64> byteLimit;
        i64 skipPrefix = 0;
        auto httpRange = NHttp::FindBytesRange(req->GetHeaders());

        YT_LOG_DEBUG("Received Skynet read request (ChunkId: %v, ReadRange: %v, StartPartIndex: %v, HttpRange: %v)",
            chunkId,
            readRange,
            startPartIndex,
            httpRange);

        if (httpRange) {
            AdjustReadRange(*httpRange, &readRange, &startPartIndex, &skipPrefix, &byteLimit);

            YT_LOG_DEBUG("Adjusted read range (ChunkId: %v, ReadRange: %v, StartPartIndex: %v, HttpRange: %v, SkipPrefix: %v, ByteLimit: %v)",
                chunkId,
                readRange,
                startPartIndex,
                httpRange,
                skipPrefix,
                byteLimit);
        }

        auto chunk = Bootstrap_->GetChunkStore()->GetChunkOrThrow(chunkId, AllMediaIndex);

        TWorkloadDescriptor skynetWorkload(EWorkloadCategory::UserBatch);
        skynetWorkload.Annotations = {"skynet"};

        TChunkReadOptions chunkReadOptions;
        chunkReadOptions.WorkloadDescriptor = skynetWorkload;
        chunkReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        chunkReadOptions.ReadSessionId = TReadSessionId::Create();

        auto chunkMeta = WaitFor(chunk->ReadMeta(chunkReadOptions))
            .ValueOrThrow();

        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
        if (!miscExt.shared_to_skynet()) {
            THROW_ERROR_EXCEPTION("Chunk access not allowed")
                << TErrorAttribute("chunk_id", chunkId);
        }
        if (readRange.LowerLimit().GetRowIndex() >= miscExt.row_count() ||
            readRange.UpperLimit().GetRowIndex() >= miscExt.row_count() + 1 ||
            readRange.LowerLimit().GetRowIndex() >= readRange.UpperLimit().GetRowIndex())
        {
            THROW_ERROR_EXCEPTION("Requested rows are out of bound")
                << TErrorAttribute("read_range", readRange)
                << TErrorAttribute("row_count", miscExt.row_count());
        }

        auto readerConfig = New<TReplicationReaderConfig>();
        auto chunkReader = CreateLocalChunkReader(
            readerConfig,
            chunk,
            Bootstrap_->GetBlockCache(),
            Bootstrap_->GetChunkMetaManager()->GetBlockMetaCache());

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = Bootstrap_->GetBlockCache(),
            .TableSchema = New<TTableSchema>(),
        });

        auto schemalessReader = CreateSchemalessRangeChunkReader(
            chunkState,
            New<TColumnarChunkMeta>(*chunkMeta),
            TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            chunkReader,
            New<TNameTable>(),
            chunkReadOptions,
            /*sortColumns*/ {},
            /*omittedInaccessibleColumns*/ {},
            /*columnFilter*/ {},
            readRange);

        auto apiReader = CreateApiFromSchemalessChunkReaderAdapter(std::move(schemalessReader));
        auto blobReader = NTableClient::CreateBlobTableReader(
            apiReader,
            TString("part_index"),
            TString("data"),
            startPartIndex);

        if (httpRange) {
            rsp->SetStatus(EStatusCode::PartialContent);
            NHttp::SetBytesRange(rsp->GetHeaders(), *httpRange);
        } else {
            rsp->SetStatus(EStatusCode::OK);
        }

        const auto& throttler = Bootstrap_->GetThrottler(NDataNode::EDataNodeThrottlerKind::SkynetOut);

        i64 byteWritten = 0;
        while (true) {
            auto blob = WaitFor(blobReader->Read())
                .ValueOrThrow();

            if (blob.Empty()) {
                if (byteLimit && byteWritten != *byteLimit) {
                    THROW_ERROR_EXCEPTION("Truncated file part")
                        << TErrorAttribute("byte_limit", byteLimit)
                        << TErrorAttribute("byte_written", byteWritten);
                }

                break;
            }

            if (skipPrefix > 0) {
                auto skipLen = Min<i64>(skipPrefix, blob.Size());
                blob = blob.Slice(skipLen, blob.Size());
                skipPrefix -= skipLen;
            }

            if (blob.Empty()) {
                continue;
            }

            WaitFor(throttler->Throttle(blob.Size()))
                .ThrowOnError();

            if (byteLimit) {
                auto blobSize = Min<i64>(blob.Size(), *byteLimit - byteWritten);
                blob = blob.Slice(0, blobSize);
            }

            byteWritten += blob.Size();
            WaitFor(rsp->Write(blob))
                .ThrowOnError();

            if (byteLimit && byteWritten == *byteLimit) {
                break;
            }
        }

        WaitFor(rsp->Close())
            .ThrowOnError();
    }

private:
    IBootstrap* Bootstrap_;
};

IHttpHandlerPtr MakeSkynetHttpHandler(IBootstrap* bootstrap)
{
    return New<TSkynetHttpHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
