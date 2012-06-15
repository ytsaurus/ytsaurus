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
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
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

inline bool CompareReaders(
    const TChunkSequenceReader* lhs,
    const TChunkSequenceReader* rhs)
{
    return CompareKeys(lhs->GetKey(), rhs->GetKey()) > 0;
}

} // namespace

TSortedMergeJob::TSortedMergeJob(
    TJobProxyConfigPtr proxyConfig,
    const TJobSpec& jobSpec)
{
    YCHECK(jobSpec.output_specs_size() == 1);

    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
    auto masterChannel = CreateLeaderChannel(proxyConfig->Masters);

    {
        TReaderOptions options;
        options.ReadKey = true;

        FOREACH (const auto& inputSpec, jobSpec.input_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            std::vector<NTableClient::NProto::TInputChunk> chunks(
                inputSpec.chunks().begin(),
                inputSpec.chunks().end());

            auto reader = New<TChunkSequenceReader>(
                proxyConfig->JobIO->ChunkSequenceReader,
                masterChannel,
                blockCache,
                chunks,
                DefaultPartitionTag,
                options);

            Readers.push_back(reader);
        }
    }

    {
        // ToDo(psushin): estimate row count for writer.
        auto asyncWriter = New<TTableChunkSequenceWriter>(
            proxyConfig->JobIO->ChunkSequenceWriter,
            ~masterChannel,
            TTransactionId::FromProto(jobSpec.output_transaction_id()),
            TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
            ChannelsFromYson(jobSpec.output_specs(0).channels()));

        Writer = CreateSyncWriter(asyncWriter);
    }
}

TJobResult TSortedMergeJob::Run()
{
    PROFILE_TIMING ("/sorted_merge_time") {
        // Open readers, remove invalid ones, and create the initial heap.
        LOG_INFO("Initializing");
        std::vector<TChunkSequenceReader*> readerHeap;
        {
            FOREACH (auto reader, Readers) {
                Sync(~reader, &TChunkSequenceReader::AsyncOpen);
                if (reader->IsValid()) {
                    readerHeap.push_back(~reader);
                }
            }

            std::make_heap(readerHeap.begin(), readerHeap.end(), CompareReaders);

            Writer->Open();
        }
        PROFILE_TIMING_CHECKPOINT("init");

        // Run the actual merge.
        LOG_INFO("Merging");
        while (!readerHeap.empty()) {
            std::pop_heap(readerHeap.begin(), readerHeap.end(), CompareReaders);
            Writer->WriteRow(readerHeap.back()->GetRow(), readerHeap.back()->GetKey());

            Sync(readerHeap.back(), &TChunkSequenceReader::AsyncNextRow);
            if (readerHeap.back()->IsValid()) {
                std::push_heap(readerHeap.begin(), readerHeap.end(), CompareReaders);
            } else {
                readerHeap.pop_back();
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
