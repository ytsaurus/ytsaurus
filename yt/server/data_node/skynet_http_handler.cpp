#include "skynet_http_handler.h"

#include "local_chunk_reader.h"

#include <yt/server/data_node/chunk_store.h>
#include <yt/server/data_node/chunk.h>

#include <yt/server/cell_node/config.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/ytlib/table_client/columnar_chunk_meta.h>

#include <yt/ytlib/api/table_reader.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/http/http.h>

#include <util/string/cgiparam.h>

namespace NYT {
namespace NDataNode {

using namespace NHttp;
using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NCellNode;
using namespace NConcurrency;

using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("SkynetHandler");

////////////////////////////////////////////////////////////////////////////////

void ParseRequest(TStringBuf rawQuery, TChunkId* chunkId, TReadRange* readRange, i64* partIndex)
{
    TCgiParameters params(rawQuery);

    if (!params.Has("chunk_id")) {
        THROW_ERROR_EXCEPTION("Missing paramenter \"chunk_id\" in URL query string.");
    }

    *chunkId = TChunkId::FromString(params.Get("chunk_id"));

    if (!params.Has("lower_row_index")) {
        THROW_ERROR_EXCEPTION("Missing paramenter \"lower_row_index\" in URL query string.");
    }
    if (!params.Has("upper_row_index")) {
        THROW_ERROR_EXCEPTION("Missing paramenter \"upper_row_index\" in URL query string.");
    }

    readRange->LowerLimit().SetRowIndex(FromString<i64>(params.Get("lower_row_index")));
    readRange->UpperLimit().SetRowIndex(FromString<i64>(params.Get("upper_row_index")));

    if (!params.Has("start_part_index")) {
        THROW_ERROR_EXCEPTION("Missing paramenter \"start_part_index\" in URL query string.");
    }

    *partIndex = FromString<i64>(params.Get("start_part_index"));

    if (*partIndex < 0 || readRange->LowerLimit().GetRowIndex() < 0 || readRange->UpperLimit().GetRowIndex() < 0) {
        THROW_ERROR_EXCEPTION("Parameter is negative")
            << TErrorAttribute("part_index", *partIndex)
            << TErrorAttribute("read_range", *readRange);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSkynetHttpHandler
    : public IHttpHandler
{
public:
    explicit TSkynetHttpHandler(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        TChunkId chunkId;
        TReadRange readRange;
        i64 startPartIndex;
        ParseRequest(req->GetUrl().RawQuery, &chunkId, &readRange, &startPartIndex);

        auto chunkPtr = Bootstrap_->GetChunkStore()->GetChunkOrThrow(chunkId, AllMediaIndex);
        auto chunkGuard = TChunkReadGuard::AcquireOrThrow(chunkPtr);

        TWorkloadDescriptor skynetWorkload(EWorkloadCategory::UserBatch);
        skynetWorkload.Annotations = {"skynet"};

        static std::vector<int> miscExtension = {
            TProtoExtensionTag<TMiscExt>::Value
        };

        TBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = skynetWorkload;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = TReadSessionId::Create();

        auto chunkMeta = WaitFor(chunkPtr->ReadMeta(blockReadOptions))
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
            chunkPtr,
            Bootstrap_->GetChunkBlockManager(),
            Bootstrap_->GetBlockCache(),
            Bootstrap_->GetBlockMetaCache() );

        auto chunkState = New<TChunkState>(
            Bootstrap_->GetBlockCache(),
            NChunkClient::NProto::TChunkSpec(),
            nullptr,
            nullptr,
            nullptr,
            nullptr);

        auto schemalessReader = CreateSchemalessChunkReader(
            chunkState,
            New<TColumnarChunkMeta>(*chunkMeta),
            New<TChunkReaderConfig>(),
            New<TChunkReaderOptions>(),
            chunkReader,
            New<TNameTable>(),
            blockReadOptions,
            TKeyColumns(),
            TColumnFilter(),
            readRange);

        auto stream = CreateBlobTableReader(
            schemalessReader,
            TString("part_index"),
            TString("data"),
            startPartIndex);

        rsp->SetStatus(EStatusCode::OK);

        auto throttler = Bootstrap_->GetSkynetOutThrottler();
        while (true) {
            auto blob = WaitFor(stream->Read())
                .ValueOrThrow();

            if (blob.Empty()) {
                break;
            }

            WaitFor(throttler->Throttle(blob.Size()))
                .ThrowOnError();

            WaitFor(rsp->Write(blob))
                .ThrowOnError();
        }

        WaitFor(rsp->Close())
            .ThrowOnError();
    }

private:
    TBootstrap* Bootstrap_;
};

IHttpHandlerPtr MakeSkynetHttpHandler(TBootstrap* bootstrap)
{
    return New<TSkynetHttpHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
