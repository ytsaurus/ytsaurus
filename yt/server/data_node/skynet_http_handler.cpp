#include "skynet_http_handler.h"

#include "local_chunk_reader.h"

#include <yt/server/data_node/chunk_store.h>
#include <yt/server/data_node/chunk.h>

#include <yt/server/cell_node/config.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/chunk_state.h>

#include <yt/ytlib/api/table_reader.h>

#include <yt/core/concurrency/async_stream.h>

#include <util/string/cgiparam.h>

namespace NYT {
namespace NDataNode {

using namespace NXHttp;
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

void ParseRequest(const TString& request, TChunkId* chunkId, TReadRange* readRange, i64* partIndex)
{
    int paramPos = request.find("?");
    if (paramPos == TString::npos) {
        THROW_ERROR_EXCEPTION("Bad query");
    }

    TString paramsString = request.substr(paramPos + 1);
    TCgiParameters params(paramsString);

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
}

TString DoReadSkynetChunk(TBootstrap* bootstrap, const TString& request)
{
    TChunkId chunkId;
    TReadRange readRange;
    i64 startPartIndex;
    ParseRequest(request, &chunkId, &readRange, &startPartIndex);

    auto chunkPtr = bootstrap->GetChunkStore()->GetChunkOrThrow(chunkId, AllMediaIndex);
    auto chunkGuard = TChunkReadGuard::AcquireOrThrow(chunkPtr);
    auto sessionId = TReadSessionId::Create();

    TWorkloadDescriptor skynetWorkload(EWorkloadCategory::UserBatch);
    skynetWorkload.Annotations = {"skynet"};
    auto throttler = bootstrap->GetOutThrottler(skynetWorkload);

    static std::vector<int> miscExtension = {
        TProtoExtensionTag<TMiscExt>::Value
    };
    auto asyncChunkMeta = chunkPtr->ReadMeta(
        skynetWorkload,
        miscExtension);
    auto chunkMeta = WaitFor(asyncChunkMeta).ValueOrThrow();

    auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
    if (!miscExt.shared_to_skynet()) {
        THROW_ERROR_EXCEPTION("Chunk access not allowed")
            << TErrorAttribute("chunk_id", chunkId);
    }

    auto readerConfig = New<TReplicationReaderConfig>();
    auto chunkReader = CreateLocalChunkReader(
        readerConfig,
        chunkPtr,
        bootstrap->GetChunkBlockManager(),
        bootstrap->GetBlockCache());

    auto chunkState = New<TChunkState>(
        bootstrap->GetBlockCache(),
        NChunkClient::NProto::TChunkSpec(),
        nullptr,
        nullptr,
        nullptr,
        nullptr);

    auto schemalessReaderConfig = New<TChunkReaderConfig>();
    schemalessReaderConfig->WorkloadDescriptor = skynetWorkload;

    auto schemalessReader = CreateSchemalessChunkReader(
        chunkState,
        schemalessReaderConfig,
        New<TChunkReaderOptions>(),
        chunkReader,
        New<TNameTable>(),
        sessionId,
        TKeyColumns(),
        TColumnFilter(),
        readRange);

    auto stream = CreateBlobTableReader(
        schemalessReader,
        TString("part_index"),
        TString("data"),
        startPartIndex);

    TString response;
    TStringOutput buffer(response);
    PipeInputToOutput(CreateCopyingAdapter(stream), &buffer, 1024);
    
    return FormatOKResponse(response);
}

////////////////////////////////////////////////////////////////////////////////

NXHttp::TServer::TAsyncHandler MakeSkynetHttpHandler(NCellNode::TBootstrap* bootstrap)
{
    return BIND([bootstrap] (const TString& request) -> TFuture<TString> {
        return BIND([bootstrap, request] {
                try {
                    return DoReadSkynetChunk(bootstrap, request);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Error executing skynet http handler");
                    return FormatInternalServerErrorResponse(ex.what());
                }
            })
            .AsyncVia(bootstrap->GetControlInvoker())
            .Run();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
