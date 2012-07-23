#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "partition_sort_job.h"
#include "small_key.h"

#include <ytlib/table_client/value.h>
#include <ytlib/table_client/partition_chunk_sequence_reader.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NChunkServer;
using namespace NElection;
using namespace NChunkClient;
using namespace NScheduler::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

TPartitionSortJob::TPartitionSortJob(
    TJobProxyConfigPtr proxyConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    YCHECK(jobSpec.input_specs_size() == 1);
    YCHECK(jobSpec.output_specs_size() == 1);

    auto masterChannel = CreateLeaderChannel(proxyConfig->Masters);
    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
    auto jobSpecExt = jobSpec.GetExtension(TSortJobSpecExt::sort_job_spec_ext);

    KeyColumns = FromProto<Stroka>(jobSpecExt.key_columns());

    TReaderOptions options;
    options.KeepBlocks = true;

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        jobSpec.input_specs(0).chunks().begin(),
        jobSpec.input_specs(0).chunks().end());

    srand(time(NULL));
    std::random_shuffle(chunks.begin(), chunks.end());

    Reader = New<TPartitionChunkSequenceReader>(
        proxyConfig->JobIO->ChunkSequenceReader, 
        masterChannel, 
        blockCache, 
        MoveRV(chunks));

    Writer = New<TTableChunkSequenceWriter>(
        proxyConfig->JobIO->ChunkSequenceWriter,
        masterChannel,
        TTransactionId::FromProto(jobSpec.output_transaction_id()),
        TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
        ChannelsFromYson(TYsonString(jobSpec.output_specs(0).channels())),
        KeyColumns);
}

TJobResult TPartitionSortJob::Run()
{
    PROFILE_TIMING ("/sort_time") {

        auto keyColumnCount = KeyColumns.size();

        std::vector<TSmallKeyPart> keyBuffer;
        std::vector<const char*> rowPtrBuffer;
        std::vector<ui32> rowIndexBuffer;

        LOG_INFO("Partition sort job.");
        {
            Sync(~Reader, &TPartitionChunkSequenceReader::AsyncOpen);

            keyBuffer.reserve(Reader->GetRowCount() * keyColumnCount);
            rowPtrBuffer.reserve(Reader->GetRowCount());
            rowIndexBuffer.reserve(Reader->GetRowCount());

            LOG_INFO("Estimated row count: %d", Reader->GetRowCount());
        }

        PROFILE_TIMING_CHECKPOINT("init");

        LOG_INFO("Reading");
        {
            TLexer lexer;
            while (Reader->IsValid()) {
                rowPtrBuffer.push_back(Reader->CurrentReader()->GetRowPointer());
                rowIndexBuffer.push_back(rowIndexBuffer.size());
                YASSERT(rowIndexBuffer.back() <= std::numeric_limits<ui32>::max());

                keyBuffer.resize(keyBuffer.size() + keyColumnCount);

                for (int i = 0; i < keyColumnCount; ++i) {
                    auto value = Reader->CurrentReader()->ReadValue(KeyColumns[i]);
                    if (!value.IsNull()) {
                        auto& keyPart = keyBuffer[rowIndexBuffer.back() * keyColumnCount + i];
                        SetSmallKeyPart(keyPart, value.ToStringBuf(), lexer);
                    }
                }

                if (!Reader->FetchNextItem()) {
                    Sync(~Reader, &TPartitionChunkSequenceReader::GetReadyEvent);
                }
            }

            LOG_INFO("Read row count: %d", static_cast<int>(rowIndexBuffer.size()));
        }
        PROFILE_TIMING_CHECKPOINT("read");

        LOG_INFO("Sorting");

        std::sort(
            rowIndexBuffer.begin(), 
            rowIndexBuffer.end(),
            [&] (ui32 lhs, ui32 rhs) -> bool {
                for (int i = 0; i < keyColumnCount; ++i) {
                    auto res = CompareSmallKeyParts(
                        keyBuffer[lhs * keyColumnCount + i], 
                        keyBuffer[rhs * keyColumnCount + i]);

                    if (res < 0)
                        return true;
                    if (res > 0)
                        return false;
                }

                return false;
        });

        PROFILE_TIMING_CHECKPOINT("sort");

        LOG_INFO("Writing");
        {
            auto writer = CreateSyncWriter(Writer);
            writer->Open();

            TMemoryInput input;
            TRow row;
            TNonOwningKey key(keyColumnCount);

            size_t progressIndex = 0;
            for (; progressIndex < rowIndexBuffer.size(); ++progressIndex) {
                row.clear();
                key.Clear();

                auto rowIndex = rowIndexBuffer[progressIndex];

                for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                    auto& keyPart = keyBuffer[rowIndex * keyColumnCount + keyIndex];
                    SetKeyPart(&key, keyPart, keyIndex);
                }

                input.Reset(rowPtrBuffer[rowIndex], std::numeric_limits<size_t>::max());
                while (true) {
                    auto value = TValue::Load(&input);
                    if (!value.IsNull()) {
                        i32 columnNameLength;
                        ReadVarInt32(&input, &columnNameLength);
                        YASSERT(columnNameLength > 0);
                        row.push_back(std::make_pair(
                            TStringBuf(input.Buf(), columnNameLength),
                            value.ToStringBuf()));

                        input.Skip(columnNameLength);
                    } else {
                        break;
                    }
                }

                writer->WriteRowUnsafe(row, key);

                if (progressIndex % 1000 == 0) {
                    Writer->SetProgress(double(progressIndex) / rowIndexBuffer.size());
                }
            }

            writer->Close();

            LOG_INFO("Written row count: %d", static_cast<int>(progressIndex));
        }

        PROFILE_TIMING_CHECKPOINT("write");

        LOG_INFO("Finalizing");
        {
            TJobResult result;
            auto* resultExt = result.MutableExtension(TSortJobResultExt::sort_job_result_ext);
            ToProto(resultExt->mutable_chunks(), Writer->GetWrittenChunks());
            *result.mutable_error() = TError().ToProto();
            return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
