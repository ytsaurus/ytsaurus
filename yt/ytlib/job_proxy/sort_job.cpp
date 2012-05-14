#include "stdafx.h"
#include "config.h"
#include "sorted_merge_job.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
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

TSortJob::TSortJob(
    const TJobIOConfigPtr& config,
    const NElection::TLeaderLookup::TConfigPtr& masterConfig,
    const NScheduler::NProto::TSortJobSpec& sortJobSpec)
{
    auto masterChannel = CreateLeaderChannel(masterConfig);

    {
        auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());
        TReaderOptions options;
        options.KeepBlocks = true;

        std::vector<NTableClient::NProto::TInputChunk> chunks(
            sortJobSpec.input_spec().chunks().begin(),
            sortJobSpec.input_spec().chunks().end());

        Reader = New<TChunkSequenceReader>(
            config->ChunkSequenceReader, 
            masterChannel, 
            blockCache, 
            chunks,
            sortJobSpec.partition_tag(),
            options);
    }

    {
        const TYson& channels = IoSpec.output_specs(index).channels();

        Writer = New<TChunkSequenceWriter>(
            config->ChunkSequenceWriter,
            masterChannel,
            TTransactionId::FromProto(sortJobSpec.output_transaction_id()),
            TChunkListId::FromProto(sortJobSpec.output_spec().chunk_list_id()),
            ChannelsFromYson(channels)));
    }
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
