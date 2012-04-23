#include "stdafx.h"
#include "config.h"
#include "ordered_merge_job.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/validating_writer.h>


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

    Reader = New<TSyncReaderAdapter>(~New<TChunkSequenceReader>(
        ~config->ChunkSequenceReader,
        ~masterChannel,
        ~blockCache,
        inputChunks));
    Reader->Open();

    // ToDo(psushin): estimate row count for writer.
    auto asyncWriter = New<TChunkSequenceWriter>(
        ~config->ChunkSequenceWriter,
        ~masterChannel,
        TTransactionId::FromProto(mergeJobSpec.output_transaction_id()),
        TChunkListId::FromProto(mergeJobSpec.output_spec().chunk_list_id()));

    Writer = New<TSyncValidatingAdaptor>(new TValidatingWriter(
        TSchema::FromYson(mergeJobSpec.output_spec().schema()), 
        ~asyncWriter));
    Writer->Open();
}

TJobResult TOrderedMergeJob::Run()
{
    while (Reader->IsValid()) {
        FOREACH (const auto& pair, Reader->GetRow()) {
            Writer->Write(pair.first, pair.second);
        }
        Writer->EndRow();
        Reader->NextRow();
    }

    Writer->Close();

    TJobResult result;
    *result.mutable_error() = TError().ToProto();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
