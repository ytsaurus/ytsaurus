#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/chunk_file_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT {

using namespace NBus;
using namespace NRpc::NBus;
using namespace NConcurrency;
using namespace NIO;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void UploadChunk(const TString& chunkId, const TString& chunkPath, const TString& nodeAddress)
{
    auto client = CreateBusClient(TBusClientConfig::CreateTcp(nodeAddress));
    auto bus = CreateBusChannel(client);

    auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYTree::CreateEphemeralNodeFactory()->CreateMap());

    auto chunkReader = New<TChunkFileReader>(
        ioEngine,
        TGuid::FromString(chunkId),
        chunkPath);

    auto meta = WaitFor(chunkReader->GetMeta({}))
        .ValueOrThrow();

    auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions());

    TDataNodeServiceProxy proxy(bus);

    TSessionId sessionId(TGuid::FromString(chunkId), 0);

    auto startChunkReq = proxy.StartChunk();
    ToProto(startChunkReq->mutable_session_id(), sessionId);

    TWorkloadDescriptor workloadDescriptor;
    workloadDescriptor.Annotations.push_back("Manual chunk upload");
    ToProto(startChunkReq->mutable_workload_descriptor(), workloadDescriptor);

    WaitFor(startChunkReq->Invoke())
        .ThrowOnError();

    std::vector<TBlock> blocks;
    int blockIndex = 0;
    i64 blockSizes = 0;

    auto flushBlocks = [&] () {
        auto putBlocksReq = proxy.PutBlocks();

        ToProto(putBlocksReq->mutable_session_id(), sessionId);
        putBlocksReq->set_first_block_index(blockIndex);

        SetRpcAttachedBlocks(putBlocksReq, blocks);

        WaitFor(putBlocksReq->Invoke())
            .ThrowOnError();

        auto flushReq = proxy.FlushBlocks();
        ToProto(flushReq->mutable_session_id(), sessionId);
        flushReq->set_block_index(blockIndex + std::ssize(blocks) - 1);

        WaitFor(flushReq->Invoke())
            .ThrowOnError();

        blockIndex += blocks.size();
        blocks.clear();
        blockSizes = 0;
    };

    for (int i = 0; i < blocksExt.blocks_size(); i++) {
        auto singleBlock = WaitFor(chunkReader->ReadBlocks(/*options*/{}, /*blockIndexes*/{i}))
            .ValueOrThrow();

        blocks.push_back(singleBlock[0]);
        blockSizes += singleBlock[0].Size();

        if (blockSizes > static_cast<i64>(16_MB)) {
            flushBlocks();
        }
    }

    if (!blocks.empty()) {
        flushBlocks();
    }

    auto finishChunkReq = proxy.FinishChunk();
    ToProto(finishChunkReq->mutable_session_id(), sessionId);
    *finishChunkReq->mutable_chunk_meta() = *meta;
    finishChunkReq->set_block_count(blocksExt.blocks_size());

    WaitFor(finishChunkReq->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    try {
        if (argc != 4) {
            Cerr << "usage: " << argv[0] << " <chunk-id> <chunk-path> <node-address>" << Endl;
            return 1;
        }

        NYT::UploadChunk(argv[1], argv[2], argv[3]);
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 1;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////
