#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "sort_job.h"

#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/ytree/lexer.h>

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler::NProto;
using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NYTree;

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TSortRow
    : public TRefCounted
{
    TRow Row;
    TNonOwningKey Key;
};

typedef TIntrusivePtr<TSortRow> TSortRowPtr;

bool operator< (const TSortRowPtr& lhs, const TSortRowPtr& rhs)
{
    return CompareKeys(lhs->Key, rhs->Key) < 0;
}

}

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

        Sync(~Reader, &TChunkSequenceReader::AsyncOpen);
    }

    {
        const TYson& channels = sortJobSpec.output_spec().channels();
        KeyColumns.assign(
            sortJobSpec.key_columns().begin(),
            sortJobSpec.key_columns().end());

        Writer = New<TChunkSequenceWriter>(
            config->ChunkSequenceWriter,
            masterChannel,
            TTransactionId::FromProto(sortJobSpec.output_transaction_id()),
            TChunkListId::FromProto(sortJobSpec.output_spec().chunk_list_id()),
            ChannelsFromYson(channels),
            KeyColumns);
        Sync(~Writer, &TChunkSequenceWriter::AsyncOpen);
    }
}

TJobResult TSortJob::Run()
{
    std::vector<TSortRowPtr> sortBuffer;

    {
        TLexer lexer;
        yhash_map<TStringBuf, int> keyColumnToIndex;

        for (int i = 0; i < KeyColumns.size(); ++i) {
            TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
            keyColumnToIndex[name] = i;
        }

        while (Reader->IsValid()) {
            auto sortRow = New<TSortRow>();
            sortRow->Row.swap(Reader->GetRow());
            sortRow->Key.Reset(KeyColumns.size());

            FOREACH(const auto& pair, sortRow->Row) {
                auto it = keyColumnToIndex.find(pair.first);
                if (it != keyColumnToIndex.end()) {
                    sortRow->Key.SetKeyPart(it->second, pair.second, lexer);
                }
            }

            sortBuffer.push_back(sortRow);
            Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
        }
    }

    LOG_DEBUG("Sort input has been read.");

    std::sort(sortBuffer.begin(), sortBuffer.end());

    LOG_DEBUG("Sort completed, start writing.");

    for (int i = 0; i < sortBuffer.size(); ++i) {
        Sync(~Writer, &TChunkSequenceWriter::AsyncWriteRow, sortBuffer[i]->Row, sortBuffer[i]->Key);
        // ToDo(psushin): Writer->SetProgress();
    }

    Sync(~Writer, &TChunkSequenceWriter::AsyncClose);


    TSortJobResult sortResult;
    ToProto(sortResult.mutable_chunks(), Writer->GetWrittenChunks());

    TJobResult result;
    *result.mutable_error() = TError().ToProto();
    *result.MutableExtension(TSortJobResult::sort_job_result) = sortResult;

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
