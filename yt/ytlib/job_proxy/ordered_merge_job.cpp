#include "stdafx.h"
#include "ordered_merge_job.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/table_client/table_chunk_sequence_reader.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

TOrderedMergeJob::TOrderedMergeJob(
    TJobProxyConfigPtr proxyConfig,
    const TJobSpec& jobSpec)
{
    YCHECK(jobSpec.output_specs_size() == 1);

    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
    auto masterChannel = CreateLeaderChannel(proxyConfig->Masters);

    std::vector<TInputChunk> inputChunks;
    FOREACH (const auto& inputSpec, jobSpec.input_specs()) {
        FOREACH (const auto& inputChunk, inputSpec.chunks()) {
            inputChunks.push_back(inputChunk);
        }
    }

    Reader = CreateSyncReader(New<TTableChunkSequenceReader>(
        proxyConfig->JobIO->ChunkSequenceReader,
        masterChannel,
        blockCache,
        MoveRV(inputChunks)));

    {
        if (jobSpec.HasExtension(TMergeJobSpecExt::merge_job_spec_ext)) {
            const auto& mergeSpec = jobSpec.GetExtension(TMergeJobSpecExt::merge_job_spec_ext);
            KeyColumns.Assign(FromProto<Stroka>(mergeSpec.key_columns()));

            LOG_INFO("Ordered merge produces sorted output.");
        }

        // ToDo(psushin): estimate row count for writer.
        Writer = New<TTableChunkSequenceWriter>(
            proxyConfig->JobIO->ChunkSequenceWriter,
            masterChannel,
            TTransactionId::FromProto(jobSpec.output_transaction_id()),
            TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
            ChannelsFromYson(NYTree::TYsonString(jobSpec.output_specs(0).channels())),
            KeyColumns);
    }
}

TJobResult TOrderedMergeJob::Run()
{
    PROFILE_TIMING ("/ordered_merge_time") {
        LOG_INFO("Initializing");

        yhash_map<TStringBuf, int> keyColumnToIndex;

        auto writer = CreateSyncWriter(Writer);
        {
            if (KeyColumns) {
                // ToDo(psushin): remove this after rewriting chunk_writer.
                for (int i = 0; i < KeyColumns->size(); ++i) {
                    TStringBuf name(~KeyColumns->at(i), KeyColumns->at(i).size());
                    keyColumnToIndex[name] = i;
                }
            }

            Reader->Open();
            writer->Open();
        }
        PROFILE_TIMING_CHECKPOINT("init");

        LOG_INFO("Merging");
        {
            NYTree::TLexer lexer;
            // Unsorted write - use dummy key.
            TNonOwningKey key;
            if (KeyColumns)
                key.ClearAndResize(KeyColumns->size());

            while (Reader->IsValid()) {
                const TRow& row = Reader->GetRow();

                if (KeyColumns) {
                    key.Clear();

                    FOREACH (const auto& pair, row) {
                        auto it = keyColumnToIndex.find(pair.first);
                        if (it != keyColumnToIndex.end()) {
                            key.SetKeyPart(it->second, pair.second, lexer);
                        }
                    }
                    writer->WriteRowUnsafe(row, key);
                } else {
                    writer->WriteRowUnsafe(row);
                }

                Reader->NextRow();
            }
        }
        PROFILE_TIMING_CHECKPOINT("merge");

        LOG_INFO("Finalizing");
        {
            writer->Close();

            TJobResult result;
            *result.mutable_error() = TError().ToProto();
            return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
