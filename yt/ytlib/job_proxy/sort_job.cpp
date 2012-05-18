#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "sort_job.h"

#include <ytlib/misc/sync.h>
#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
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

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TSortRow
{
    size_t EndValueIndex;
    TNonOwningKey Key;
};

bool operator < (const TSortRow& lhs, const TSortRow& rhs)
{
    return CompareKeys(lhs.Key, rhs.Key) < 0;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSortJob::TSortJob(
    TJobIOConfigPtr ioConfig,
    NElection::TLeaderLookup::TConfigPtr masterConfig,
    const NScheduler::NProto::TSortJobSpec& jobSpec)
{
    auto masterChannel = CreateLeaderChannel(masterConfig);

    KeyColumns.assign(
        jobSpec.key_columns().begin(),
        jobSpec.key_columns().end());

    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

    TReaderOptions options;
    options.KeepBlocks = true;

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        jobSpec.input_spec().chunks().begin(),
        jobSpec.input_spec().chunks().end());

    Reader = New<TChunkSequenceReader>(
        ioConfig->ChunkSequenceReader, 
        masterChannel, 
        blockCache, 
        chunks,
        jobSpec.partition_tag(),
        options);

    Writer = New<TTableChunkSequenceWriter>(
        ioConfig->ChunkSequenceWriter,
        masterChannel,
        TTransactionId::FromProto(jobSpec.output_transaction_id()),
        TChunkListId::FromProto(jobSpec.output_spec().chunk_list_id()),
        ChannelsFromYson(jobSpec.output_spec().channels()),
        KeyColumns);
}

TJobResult TSortJob::Run()
{
    PROFILE_TIMING ("/sort_time") {
        LOG_INFO("Initializing");

        yhash_map<TStringBuf, int> keyColumnToIndex;

        for (int i = 0; i < KeyColumns.size(); ++i) {
            TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
            keyColumnToIndex[name] = i;
        }

        Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

        PROFILE_TIMING_CHECKPOINT("init");

        // TODO(babenko): fix reserve below once the reader can provide per-partition statistics

        std::vector< std::pair<TStringBuf, TStringBuf> > valueBuffer;
        //valueBuffer.reserve(Reader->GetValueCount());
        valueBuffer.reserve(1000000);

        std::vector<TSortRow> rowBuffer;
        //rowBuffer.reserve(Reader->GetRowCount());
        rowBuffer.reserve(1000000);

        std::vector<size_t> indexBuffer;
        //indexBuffer.reserve(Reader->GetRowCount());
        indexBuffer.reserve(1000000);

        LOG_INFO("Reading");
        {
            TLexer lexer;
            while (Reader->IsValid()) {
                TSortRow sortRow;
                sortRow.Key.Reset(KeyColumns.size());

                FOREACH (const auto& pair, Reader->GetRow()) {
                    auto it = keyColumnToIndex.find(pair.first);
                    if (it != keyColumnToIndex.end()) {
                        sortRow.Key.SetKeyPart(it->second, pair.second, lexer);
                    }
                    valueBuffer.push_back(pair);
                }

                sortRow.EndValueIndex = valueBuffer.size();

                indexBuffer.push_back(rowBuffer.size());
                rowBuffer.push_back(sortRow);

                Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
            }
        }
        PROFILE_TIMING_CHECKPOINT("read");

        LOG_INFO("Sorting");

        std::sort(
            indexBuffer.begin(), 
            indexBuffer.end(),
            [&] (size_t lhs, size_t rhs) {
                return CompareKeys(rowBuffer[lhs].Key, rowBuffer[rhs].Key) < 0;
            }
        );

        PROFILE_TIMING_CHECKPOINT("sort");

        LOG_INFO("Writing");
        {
            Sync(~Writer, &TTableChunkSequenceWriter::AsyncOpen);

            TRow row;
            for (size_t i = 0; i < indexBuffer.size(); ++i) {
                size_t index = indexBuffer[i];
                row.clear();

                auto& sortRow = rowBuffer[index];
                for (size_t valueIndex = index > 0 ? rowBuffer[index - 1].EndValueIndex : 0; 
                    valueIndex < sortRow.EndValueIndex; 
                    ++valueIndex) 
                {
                    row.push_back(valueBuffer[valueIndex]);
                }

                Writer->SetProgress(double(i) / indexBuffer.size());
                Sync(~Writer, &TTableChunkSequenceWriter::AsyncWriteRow, row, sortRow.Key);
            }

            Sync(~Writer, &TTableChunkSequenceWriter::AsyncClose);
        }

        PROFILE_TIMING_CHECKPOINT("write");

        LOG_INFO("Finalizing");
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
