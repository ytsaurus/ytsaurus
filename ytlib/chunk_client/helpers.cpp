#include "helpers.h"
#include "private.h"
#include "config.h"
#include "erasure_reader.h"
#include "replication_reader.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/chunk_ypath_proxy.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NObjectClient;
using namespace NErasure;
using namespace NApi;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<TMasterYPathProxy::TRspCreateObjectsPtr> CreateChunk(
    NRpc::IChannelPtr masterChannel,
    TMultiChunkWriterOptionsPtr options,
    const TTransactionId& transactionId,
    const TChunkListId& chunkListId)
{
    LOG_DEBUG(
        "Creating chunk (ReplicationFactor: %v, TransactionId: %v)",
        options->ReplicationFactor,
        transactionId);

    TObjectServiceProxy objectProxy(masterChannel);

    auto chunkType = options->ErasureCodec == ECodec::None
         ? EObjectType::Chunk
         : EObjectType::ErasureChunk;

    auto req = TMasterYPathProxy::CreateObjects();
    ToProto(req->mutable_transaction_id(), transactionId);
    GenerateMutationId(req);
    req->set_type(static_cast<int>(chunkType));
    req->set_account(options->Account);

    auto* reqExt = req->MutableExtension(NProto::TReqCreateChunkExt::create_chunk_ext);
    reqExt->set_replication_factor(options->ReplicationFactor);
    reqExt->set_movable(options->ChunksMovable);
    reqExt->set_vital(options->ChunksVital);
    reqExt->set_erasure_codec(static_cast<int>(options->ErasureCodec));
    if (chunkListId != NullChunkListId) {
        ToProto(reqExt->mutable_chunk_list_id(), chunkListId);
    }

    return objectProxy.Execute(req);
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRemoteReader(
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    NErasure::ECodec erasureCodecId,
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler)
{
    LOG_DEBUG("Creating remote reader (ChunkId: %v)", chunkId);

    if (IsErasureChunkId(chunkId)) {
        auto sortedReplicas = replicas;
        std::sort(
            sortedReplicas.begin(),
            sortedReplicas.end(),
            [] (TChunkReplica lhs, TChunkReplica rhs) {
                return lhs.GetIndex() < rhs.GetIndex();
            });

        auto* erasureCodec = GetCodec(erasureCodecId);
        auto dataPartCount = erasureCodec->GetDataPartCount();

        std::vector<IChunkReaderPtr> readers;
        readers.reserve(dataPartCount);

        auto it = sortedReplicas.begin();
        while (it != sortedReplicas.end() && it->GetIndex() < dataPartCount) {
            auto jt = it;
            while (jt != sortedReplicas.end() && it->GetIndex() == jt->GetIndex()) {
                ++jt;
            }

            TChunkReplicaList partReplicas(it, jt);
            auto partId = ErasurePartIdFromChunkId(chunkId, it->GetIndex());
            auto reader = CreateReplicationReader(
                config,
                options,
                client,
                nodeDirectory,
                Null,
                partId,
                partReplicas,
                blockCache,
                throttler);
            readers.push_back(reader);

            it = jt;
        }

        YCHECK(readers.size() == dataPartCount);
        return CreateNonRepairingErasureReader(readers);
    } else {
        return CreateReplicationReader(
            config,
            options,
            client,
            nodeDirectory,
            Null,
            chunkId,
            replicas,
            blockCache,
            throttler);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
