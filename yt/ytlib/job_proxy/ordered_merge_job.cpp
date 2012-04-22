#include "stdafx.h"
#include "config.h"
#include "ordered_merge_job.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>


namespace NYT {
namespace NJobProxy {

using namespace NScheduler::NProto;
using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TOrderedMergeJob::TOrderedMergeJob(
    const TJobIOConfigPtr& config,
    const NElection::TLeaderLookup::TConfigPtr& masterConfig,
    const NScheduler::NProto::TMergeJobSpec& mergeJobSpec)
{
    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());
    auto masterChannel = CreateLeaderChannel(masterConfig);

    auto inputChunks = FromProto<NTableClient::NProto::TInputChunk>(
        mergeJobSpec.input_spec().chunks());

    Reader = New<TSyncReaderAdapter>(New<TChunkSequenceReader>(
        config->ChunkSequenceReader,
        masterChannel,
        blockCache,
        inputChunks));
    Reader->Open();

    // ToDo(psushin): estimate row count for writer.
    auto asyncWriter = New<TChunkSequenceWriter>(
        config->ChunkSequenceWriter,
        masterChannel,
        TTransactionId::FromProto(mergeJobSpec.output_transaction_id()),
        TChunkListId::FromProto(mergeJobSpec.output_spec().chunk_list_id()),
        ChannelsFromYson(mergeJobSpec.output_spec().channels()));

    Writer = New<TSyncWriterAdapter>(asyncWriter);
    Writer->Open();
}

TJobResult TOrderedMergeJob::Run()
{
    // Unsorted write - use dummy key.
    TKey key;
    while (Reader->IsValid()) {
        Writer->WriteRow(Reader->GetRow(), key);
    }

    Writer->Close();

    TJobResult result;
    *result.mutable_error() = TError().ToProto();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
