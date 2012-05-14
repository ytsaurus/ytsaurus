#include "stdafx.h"
#include "config.h"
#include "sorted_merge_job.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/private.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_reader.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler::NProto;
using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

inline bool CompareReaders(
    const TChunkReaderPtr& r1, 
    const TChunkReaderPtr& r2)
{
    return CompareKeys(r1->GetKey(), r2->GetKey()) > 0;
}

} // namespace

TSortedMergeJob::TSortedMergeJob(
    const TJobIOConfigPtr& config,
    const NElection::TLeaderLookup::TConfigPtr& masterConfig,
    const NScheduler::NProto::TMergeJobSpec& mergeJobSpec)
{
    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());
    auto masterChannel = CreateLeaderChannel(masterConfig);

    for (int i = 0; i < mergeJobSpec.input_spec().chunks_size(); ++i) {
        // ToDo(psushin): validate that input chunks are sorted.

        const auto& inputChunk = mergeJobSpec.input_spec().chunks(i);
        yvector<Stroka> seedAddresses = FromProto<Stroka>(inputChunk.holder_addresses());

        auto remoteReader = CreateRemoteReader(
            config->ChunkSequenceReader->RemoteReader,
            blockCache,
            ~masterChannel,
            TChunkId::FromProto(inputChunk.slice().chunk_id()),
            seedAddresses);

        TReaderOptions options;
        options.ReadKey = true;

        auto chunkReader = New<TChunkReader>(
            config->ChunkSequenceReader->SequentialReader,
            TChannel::CreateUniversal(),
            remoteReader,
            inputChunk.slice().start_limit(),
            inputChunk.slice().end_limit(),
            "", // No row attributes.
            DefaultPartitionTag,
            options); 

        ChunkReaders.push_back(chunkReader);
        Sync(~ChunkReaders.back(), &TChunkReader::AsyncOpen);

        if (!ChunkReaders.back()->IsValid()) {
            ChunkReaders.pop_back();
        }
    }

    std::make_heap(ChunkReaders.begin(), ChunkReaders.end(), CompareReaders);

    // ToDo(psushin): estimate row count for writer.
    auto asyncWriter = New<TChunkSequenceWriter>(
        ~config->ChunkSequenceWriter,
        ~masterChannel,
        TTransactionId::FromProto(mergeJobSpec.output_transaction_id()),
        TChunkListId::FromProto(mergeJobSpec.output_spec().chunk_list_id()),
        ChannelsFromYson(mergeJobSpec.output_spec().channels()));

    Writer = New<TSyncWriterAdapter>(asyncWriter);

    Writer->Open();
}

TJobResult TSortedMergeJob::Run()
{
    while (!ChunkReaders.empty()) {
        std::pop_heap(ChunkReaders.begin(), ChunkReaders.end(), CompareReaders);
        Writer->WriteRow(ChunkReaders.back()->GetRow(), ChunkReaders.back()->GetKey());

        Sync(~ChunkReaders.back(), &TChunkReader::AsyncNextRow);
        if (ChunkReaders.back()->IsValid()) {
            std::push_heap(ChunkReaders.begin(), ChunkReaders.end(), CompareReaders);
        } else {
            ChunkReaders.pop_back();
        }
    }
    Writer->Close();

    TJobResult result;
    *result.mutable_error() = TError().ToProto();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
