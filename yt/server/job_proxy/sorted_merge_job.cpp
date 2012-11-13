#include "stdafx.h"
#include "sorted_merge_job.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/private.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/multi_chunk_sequential_reader.h>
#include <ytlib/table_client/merging_reader.h>

#include <ytlib/ytree/yson_string.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeJob
    : public TJob
{
public:
    explicit TSortedMergeJob(IJobHost* host)
        : TJob(host)
    {
        const auto& jobSpec = Host->GetJobSpec();
        auto config = Host->GetConfig();

        YCHECK(jobSpec.output_specs_size() == 1);

        auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
        auto masterChannel = CreateLeaderChannel(config->Masters);

        {
            std::vector<TTableChunkSequenceReaderPtr> readers;
            TReaderOptions options;
            options.ReadKey = true;

            auto provider = New<TTableChunkReaderProvider>(config->JobIO->TableReader, options);

            FOREACH (const auto& inputSpec, jobSpec.input_specs()) {
                // ToDo(psushin): validate that input chunks are sorted.
                std::vector<NTableClient::NProto::TInputChunk> chunks(
                    inputSpec.chunks().begin(),
                    inputSpec.chunks().end());

                auto reader = New<TTableChunkSequenceReader>(
                    config->JobIO->TableReader,
                    masterChannel,
                    blockCache,
                    MoveRV(chunks),
                    provider);

                readers.push_back(reader);
            }

            Reader = CreateMergingReader(readers);
        }

        {
            const auto& mergeSpec = jobSpec.GetExtension(TMergeJobSpecExt::merge_job_spec_ext); 

            // ToDo(psushin): estimate row count for writer.
            Writer = New<TTableChunkSequenceWriter>(
                config->JobIO->TableWriter,
                masterChannel,
                TTransactionId::FromProto(jobSpec.output_transaction_id()),
                TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
                ConvertTo<TChannels>(NYTree::TYsonString(jobSpec.output_specs(0).channels())),
                FromProto<Stroka>(mergeSpec.key_columns()));
        }
    }

    virtual NScheduler::NProto::TJobResult Run() override
    {
        PROFILE_TIMING ("/sorted_merge_time") {
            auto writer = CreateSyncWriter(Writer);

            // Open readers, remove invalid ones, and create the initial heap.
            LOG_INFO("Initializing");
            {
                Reader->Open();
                writer->Open();
            }
            PROFILE_TIMING_CHECKPOINT("init");

            // Run the actual merge.
            LOG_INFO("Merging");
            while (const TRow* row = Reader->GetRow()) {
                writer->WriteRowUnsafe(*row, Reader->GetKey());
            }
            PROFILE_TIMING_CHECKPOINT("merge");

            LOG_INFO("Finalizing");
            {
                writer->Close();

                TJobResult result;
                ToProto(result.mutable_error(), TError());
                return result;
            }
        }
    }

    double GetProgress() const override
    {
        i64 total = Reader->GetRowCount();
        if (total == 0) {
            LOG_WARNING("GetProgress: empty total");
            return 0;
        } else {
            double progress = (double) Reader->GetRowIndex() / total;
            LOG_DEBUG("GetProgress: %lf", progress);
            return progress;
        }
    }

private:
    ISyncReaderPtr Reader;
    TTableChunkSequenceWriterPtr Writer;

};

TJobPtr CreateSortedMergeJob(IJobHost* host)
{
    return New<TSortedMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
