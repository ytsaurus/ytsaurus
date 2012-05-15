#include "stdafx.h"
#include "sorted_merge_job.h"
#include "private.h"
#include "config.h"

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

using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

namespace {

inline bool CompareReaders(const TChunkReaderPtr& lhs, const TChunkReaderPtr& rhs)
{
    return CompareKeys(lhs->GetKey(), rhs->GetKey()) > 0;
}

} // namespace

TSortedMergeJob::TSortedMergeJob(
    TJobIOConfigPtr ioConfig,
    NElection::TLeaderLookup::TConfigPtr masterConfig,
    const NScheduler::NProto::TMergeJobSpec& jobSpec)
{
    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());
    auto masterChannel = CreateLeaderChannel(masterConfig);

    YUNREACHABLE();
    // TODO(babenko): use multiple input_spec
    //for (int i = 0; i < jobSpec.input_spec().chunks_size(); ++i) {
    //    // ToDo(psushin): validate that input chunks are sorted.

    //    const auto& inputChunk = jobSpec.input_spec().chunks(i);
    //    yvector<Stroka> seedAddresses = FromProto<Stroka>(inputChunk.holder_addresses());

    //    auto remoteReader = CreateRemoteReader(
    //        ioConfig->ChunkSequenceReader->RemoteReader,
    //        blockCache,
    //        ~masterChannel,
    //        TChunkId::FromProto(inputChunk.slice().chunk_id()),
    //        seedAddresses);

    //    TReaderOptions options;
    //    options.ReadKey = true;

    //    auto chunkReader = New<TChunkReader>(
    //        ioConfig->ChunkSequenceReader->SequentialReader,
    //        TChannel::CreateUniversal(),
    //        remoteReader,
    //        inputChunk.slice().start_limit(),
    //        inputChunk.slice().end_limit(),
    //        "", // No row attributes.
    //        DefaultPartitionTag,
    //        options); 

    //    ChunkReaders.push_back(chunkReader);
    //}

    // ToDo(psushin): estimate row count for writer.
    auto asyncWriter = New<TTableChunkSequenceWriter>(
        ~ioConfig->ChunkSequenceWriter,
        ~masterChannel,
        TTransactionId::FromProto(jobSpec.output_transaction_id()),
        TChunkListId::FromProto(jobSpec.output_spec().chunk_list_id()),
        ChannelsFromYson(jobSpec.output_spec().channels()));

    Writer = New<TSyncWriterAdapter>(asyncWriter);
}

TJobResult TSortedMergeJob::Run()
{
    PROFILE_TIMING ("/sorted_merge_time") {
        // Open readers, remove invalid ones, and create the initial heap.
        LOG_INFO("Initializing");
        {
            std::vector<TChunkReaderPtr> validChunkReaders;
            FOREACH (auto reader, ChunkReaders) {
                Sync(~reader, &TChunkReader::AsyncOpen);
                if (reader->IsValid()) {
                    validChunkReaders.push_back(reader);
                }
            }

            std::make_heap(validChunkReaders.begin(), validChunkReaders.end(), CompareReaders);
            ChunkReaders = MoveRV(validChunkReaders);

            Writer->Open();
        }
        PROFILE_TIMING_CHECKPOINT("init");

        // Run the actual merge.
        LOG_INFO("Merging");
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
        PROFILE_TIMING_CHECKPOINT("merge");

        LOG_INFO("Finalizing");
        {
            Writer->Close();

            TJobResult result;
            *result.mutable_error() = TError().ToProto();
            return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
