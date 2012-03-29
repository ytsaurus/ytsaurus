#include "stdafx.h"
#include "config.h"
#include "merge_job.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/sorted_validating_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler::NProto;
using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TMergeJob::TMergeJob(
    const TJobIoConfigPtr& config,
    const NElection::TLeaderLookup::TConfig::TPtr& masterConfig,
    const NScheduler::NProto::TMergeJobSpec& mergeJobSpec)
{
    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());
    auto masterChannel = CreateLeaderChannel(masterConfig);

    for (int i = 0; i < mergeJobSpec.input_chunks_size(); ++i) {
        // ToDo(psushin): validate that input chunks are sorted.

        const auto& inputChunk = mergeJobSpec.input_chunks(i);
        yvector<Stroka> seedAddresses = FromProto<Stroka>(inputChunk.holder_addresses());

        auto remoteReader = CreateRemoteReader(
            ~config->ChunkSequenceReader->RemoteReader,
            ~blockCache,
            ~masterChannel,
            TChunkId::FromProto(inputChunk.slice().chunk_id()),
            seedAddresses);

        auto chunkReader = New<TChunkReader>(
            ~config->ChunkSequenceReader->SequentialReader,
            TChannel::CreateUniversal(),
            ~remoteReader,
            inputChunk.slice().start_limit(),
            inputChunk.slice().end_limit(),
            ""); // No row attributes.

        ChunkReaders.push_back(New<TSyncReader>(~chunkReader));
        ChunkReaders.back()->Open();
        if (!ChunkReaders.back()->IsValid()) {
            ChunkReaders.pop_back();
        }
    }

    auto asyncWriter = New<TChunkSequenceWriter>(
        ~config->ChunkSequenceWriter,
        ~masterChannel,
        TTransactionId::FromProto(mergeJobSpec.output_transaction_id()),
        TChunkListId::FromProto(mergeJobSpec.output_chunk_list_id()));

    Writer = New<TSyncWriter>(new TSortedValidatingWriter(
        TSchema::FromYson(mergeJobSpec.schema()), 
        ~asyncWriter));

    Writer->Open();
}

TJobResult TMergeJob::Run()
{
    YUNIMPLEMENTED();
    return TJobResult();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
