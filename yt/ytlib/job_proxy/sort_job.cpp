#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "sort_job.h"

#include <ytlib/misc/sync.h>
#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NScheduler::NProto;

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TSortRow
{
    TRow Row;
    TNonOwningKey Key;
};

bool operator < (const TSortRow& lhs, const TSortRow& rhs)
{
    return CompareKeys(lhs.Key, rhs.Key) < 0;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSortJob::TSortJob(
    const TJobIOConfigPtr& ioConfig,
    const NElection::TLeaderLookup::TConfigPtr& masterConfig,
    const NScheduler::NProto::TSortJobSpec& jobSpec)
    : IOConfig(ioConfig)
    , MasterConfig(masterConfig)
    , JobSpec(jobSpec)
{ }

TJobResult TSortJob::Run()
{
    PROFILE_TIMING ("/sort_time") {
        LOG_INFO("Initializing sort");

        auto masterChannel = CreateLeaderChannel(MasterConfig);
      
        KeyColumns.assign(
            JobSpec.key_columns().begin(),
            JobSpec.key_columns().end());

        std::vector<TSortRow> sortBuffer;
        // TODO(babenko): call sortBuffer.reserve

        LOG_INFO("Reading sort buffer");
        {
            auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

            TReaderOptions options;
            options.KeepBlocks = true;

            std::vector<NTableClient::NProto::TInputChunk> chunks(
                JobSpec.input_spec().chunks().begin(),
                JobSpec.input_spec().chunks().end());

            Reader = New<TChunkSequenceReader>(
                IOConfig->ChunkSequenceReader, 
                masterChannel, 
                blockCache, 
                chunks,
                JobSpec.partition_tag(),
                options);

            Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

            TLexer lexer;
            yhash_map<TStringBuf, int> keyColumnToIndex;

            for (int i = 0; i < KeyColumns.size(); ++i) {
                TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
                keyColumnToIndex[name] = i;
            }


            while (Reader->IsValid()) {
                sortBuffer.push_back(TSortRow());
                auto& sortRow = sortBuffer.back();

                sortRow.Row.swap(Reader->GetRow());
                sortRow.Key.Reset(KeyColumns.size());

                FOREACH (const auto& pair, sortRow.Row) {
                    auto it = keyColumnToIndex.find(pair.first);
                    if (it != keyColumnToIndex.end()) {
                        sortRow.Key.SetKeyPart(it->second, pair.second, lexer);
                    }
                }

                Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
            }

        }
        PROFILE_TIMING_CHECKPOINT("/read");

        LOG_INFO("Sorting rows");
        std::sort(sortBuffer.begin(), sortBuffer.end());
        PROFILE_TIMING_CHECKPOINT("/sort");

        LOG_INFO("Writing sort buffer");
        {
            Writer = New<TChunkSequenceWriter>(
                IOConfig->ChunkSequenceWriter,
                masterChannel,
                TTransactionId::FromProto(JobSpec.output_transaction_id()),
                TChunkListId::FromProto(JobSpec.output_spec().chunk_list_id()),
                ChannelsFromYson(JobSpec.output_spec().channels()),
                KeyColumns);
            Sync(~Writer, &TChunkSequenceWriter::AsyncOpen);

            for (int i = 0; i < sortBuffer.size(); ++i) {
                Sync(~Writer, &TChunkSequenceWriter::AsyncWriteRow, sortBuffer[i].Row, sortBuffer[i].Key);
                // ToDo(psushin): Writer->SetProgress();
            }

            Sync(~Writer, &TChunkSequenceWriter::AsyncClose);
        }
        PROFILE_TIMING_CHECKPOINT("/write");

        LOG_INFO("Sort complete");
        {
            TSortJobResult sortResult;
            ToProto(sortResult.mutable_chunks(), Writer->GetWrittenChunks());

            TJobResult result;
            *result.mutable_error() = TError().ToProto();
            *result.MutableExtension(TSortJobResult::sort_job_result) = sortResult;

            return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
