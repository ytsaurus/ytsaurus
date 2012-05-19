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
        struct TSortRow
        {
            size_t EndValueIndex;
            TNonOwningKey Key;
        };

        yhash_map<TStringBuf, int> keyColumnToIndex;
        std::vector< std::pair<TStringBuf, TStringBuf> > valueBuffer;
        std::vector<TSortRow> rowBuffer;
        std::vector<ui32> indexBuffer;

        LOG_INFO("Initializing");
        {
            for (int i = 0; i < KeyColumns.size(); ++i) {
                TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
                keyColumnToIndex[name] = i;
            }

            Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

            // TODO(babenko): fix reserve below once the reader can provide per-partition statistics

            //valueBuffer.reserve(Reader->GetValueCount());
            valueBuffer.reserve(1000000);

            //rowBuffer.reserve(Reader->GetRowCount());
            rowBuffer.reserve(1000000);

            //indexBuffer.reserve(Reader->GetRowCount());
            indexBuffer.reserve(1000000);

            // Add fake row.
            TSortRow firstFakeRow;
            firstFakeRow.EndValueIndex = 0;
            rowBuffer.push_back(firstFakeRow);
        }

        PROFILE_TIMING_CHECKPOINT("init");

        LOG_INFO("Reading");
        {
            TLexer lexer;
            while (Reader->IsValid()) {
                size_t rowIndex = rowBuffer.size();

                // Avoid constructing row on stack and then copying it into the buffer.
                // TODO(babenko): consider using emplace_back
                rowBuffer.push_back(TSortRow());
                auto& sortRow = rowBuffer.back();

                sortRow.Key.Reset(KeyColumns.size());

                FOREACH (const auto& pair, Reader->GetRow()) {
                    auto it = keyColumnToIndex.find(pair.first);
                    if (it != keyColumnToIndex.end()) {
                        sortRow.Key.SetKeyPart(it->second, pair.second, lexer);
                    }
                    valueBuffer.push_back(pair);
                }

                sortRow.EndValueIndex = valueBuffer.size();

                YASSERT(rowIndex <= std::numeric_limits<ui32>::max());
                indexBuffer.push_back(rowIndex);

                Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
            }
        }
        PROFILE_TIMING_CHECKPOINT("read");

        LOG_INFO("Sorting");

        std::sort(
            indexBuffer.begin(), 
            indexBuffer.end(),
            [&] (ui32 lhs, ui32 rhs) {
                return CompareKeys(rowBuffer[lhs].Key, rowBuffer[rhs].Key) < 0;
            }
        );

        PROFILE_TIMING_CHECKPOINT("sort");

        LOG_INFO("Writing");
        {
            Sync(~Writer, &TTableChunkSequenceWriter::AsyncOpen);

            TRow row;
            for (size_t progressIndex = 0; progressIndex < indexBuffer.size(); ++progressIndex) {
                size_t rowIndex = indexBuffer[progressIndex];
                row.clear();

                const auto& sortRow = rowBuffer[rowIndex];
                for (size_t valueIndex = rowBuffer[rowIndex - 1].EndValueIndex;
                     valueIndex < sortRow.EndValueIndex; 
                     ++valueIndex) 
                {
                    row.push_back(valueBuffer[valueIndex]);
                }

                Sync(~Writer, &TTableChunkSequenceWriter::AsyncWriteRow, row, sortRow.Key);

                if (progressIndex % 1000 == 0) {
                    Writer->SetProgress(double(progressIndex) / indexBuffer.size());
                }
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
