#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "partition_sort_job.h"
#include "small_key.h"

#include <ytlib/table_client/value.h>
#include <ytlib/table_client/partition_chunk_sequence_reader.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/meta_state/leader_channel.h>
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

class TPartitionSortJob
    : public TJob
{
public:
    explicit TPartitionSortJob(IJobHost* host)
        : TJob(host)
    {
        const auto& jobSpec = Host->GetJobSpec();
        auto config = Host->GetConfig();

        YCHECK(jobSpec.input_specs_size() == 1);
        YCHECK(jobSpec.output_specs_size() == 1);

        auto masterChannel = CreateLeaderChannel(config->Masters);
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
            config->JobIO->ChunkSequenceReader, 
            masterChannel, 
            blockCache, 
            MoveRV(chunks));

        Writer = New<TTableChunkSequenceWriter>(
            config->JobIO->ChunkSequenceWriter,
            masterChannel,
            TTransactionId::FromProto(jobSpec.output_transaction_id()),
            TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
            ChannelsFromYson(TYsonString(jobSpec.output_specs(0).channels())),
            KeyColumns);
    }

    virtual NScheduler::NProto::TJobResult Run() OVERRIDE
    {
        PROFILE_TIMING ("/sort_time") {

            auto keyColumnCount = KeyColumns.size();

            std::vector<TSmallKeyPart> keyBuffer;
            std::vector<const char*> rowPtrBuffer;
            std::vector<ui32> rowIndexHeap;
            i64 estimatedRowCount = Reader->GetRowCount();

            LOG_INFO("Initializing");
            {
                Sync(~Reader, &TPartitionChunkSequenceReader::AsyncOpen);

                keyBuffer.reserve(estimatedRowCount * keyColumnCount);
                rowPtrBuffer.reserve(estimatedRowCount);
                rowIndexHeap.reserve(estimatedRowCount);

                LOG_INFO("Estimated row count: %" PRId64, estimatedRowCount);
            }
            PROFILE_TIMING_CHECKPOINT("init");

            // comparer(x, y) returns True iff row[x] < row[y]
            auto comparer = [&] (ui32 lhs, ui32 rhs) -> bool {
                int lhsStartIndex = lhs * keyColumnCount;
                int lhsEndIndex   = lhsStartIndex + keyColumnCount;
                int rhsStartIndex = rhs * keyColumnCount;
                for (int lhsIndex = lhsStartIndex, rhsIndex = rhsStartIndex;
                    lhsIndex < lhsEndIndex;
                    ++lhsIndex, ++rhsIndex)
                {
                    auto res = CompareSmallKeyParts(keyBuffer[lhsIndex], keyBuffer[rhsIndex]);
                    if (res > 0)
                        return true;
                    if (res < 0)
                        return false;
                }
                return false;
            };

            LOG_INFO("Reading");
            {
                bool isNetworkReleased = false;
                auto jobSpecExt = Host->GetJobSpec().GetExtension(TSortJobSpecExt::sort_job_spec_ext);
                auto releaseNetwork = [&] () {
                    auto utilization = Host->GetResourceUtilization();
                    utilization.set_network(0);
                    Host->SetResourceUtilization(utilization);
                };

                TLexer lexer;
                while (Reader->IsValid()) {
                    // Push row pointer.
                    rowPtrBuffer.push_back(Reader->CurrentReader()->GetRowPointer());
                    rowIndexHeap.push_back(rowIndexHeap.size());
                    YASSERT(rowIndexHeap.back() <= std::numeric_limits<ui32>::max());

                    // Push key.
                    keyBuffer.resize(keyBuffer.size() + keyColumnCount);
                    for (int i = 0; i < keyColumnCount; ++i) {
                        auto value = Reader->CurrentReader()->ReadValue(KeyColumns[i]);
                        if (!value.IsNull()) {
                            auto& keyPart = keyBuffer[rowIndexHeap.back() * keyColumnCount + i];
                            SetSmallKeyPart(keyPart, value.ToStringBuf(), lexer);
                        }
                    }

                    // Push row index and readjust the heap.
                    std::push_heap(rowIndexHeap.begin(), rowIndexHeap.end(), comparer);

                    if (!isNetworkReleased && Reader->IsFetchingComplete()) {
                        releaseNetwork();
                        isNetworkReleased =  true;
                    }

                    if (!Reader->FetchNextItem()) {
                        Sync(~Reader, &TPartitionChunkSequenceReader::GetReadyEvent);
                    }
                }

                if (!isNetworkReleased) {
                    releaseNetwork();
                }
            }
            PROFILE_TIMING_CHECKPOINT("read");

            i64 totalRowCount = rowIndexHeap.size();
            LOG_INFO("Total row count: %" PRId64, totalRowCount);

            LOG_INFO("Writing");
            {
                auto syncWriter = CreateSyncWriter(Writer);
                syncWriter->Open();

                TMemoryInput input;
                TRow row;
                TNonOwningKey key(keyColumnCount);
                bool isRowReady = false;

                auto prepareRow = [&] () {
                    YASSERT(!rowIndexHeap.empty());

                    auto rowIndex = rowIndexHeap.back();
                    rowIndexHeap.pop_back();

                    // Prepare key.
                    key.Clear();
                    for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                        auto& keyPart = keyBuffer[rowIndex * keyColumnCount + keyIndex];
                        SetKeyPart(&key, keyPart, keyIndex);
                    }

                    // Prepare row.
                    row.clear();
                    input.Reset(rowPtrBuffer[rowIndex], std::numeric_limits<size_t>::max());
                    while (true) {
                        auto value = TValue::Load(&input);
                        if (value.IsNull()) {
                            break;
                        }

                        i32 columnNameLength;
                        ReadVarInt32(&input, &columnNameLength);
                        YASSERT(columnNameLength > 0);
                        row.push_back(std::make_pair(
                            TStringBuf(input.Buf(), columnNameLength),
                            value.ToStringBuf()));
                        input.Skip(columnNameLength);
                    }
                };

                i64 writtenRowCount = 0;
                auto setProgress = [&] () {
                    if (writtenRowCount % 1000 == 0) {
                        Writer->SetProgress((double) writtenRowCount / totalRowCount);
                    }
                };

                auto heapBegin = rowIndexHeap.begin();
                auto heapEnd = rowIndexHeap.end();
                // Pop heap and do async writing.
                while (heapBegin != heapEnd) {
                    // Pop row index and readjust the heap.
                    std::pop_heap(heapBegin, heapEnd, comparer);
                    --heapEnd;

                    do {
                        if (!isRowReady) {
                            prepareRow();
                            isRowReady = true;
                        }

                        if (!Writer->TryWriteRowUnsafe(row, key)) {
                            break;
                        }

                        isRowReady = false;
                        ++writtenRowCount;

                        setProgress();

                    } while (heapEnd != rowIndexHeap.end());
                }

                YCHECK(isRowReady || rowIndexHeap.empty());

                if (isRowReady) {
                    syncWriter->WriteRowUnsafe(row, key);
                    ++writtenRowCount;
                }

                // Synchronously write the rest of the rows.
                while (!rowIndexHeap.empty()) {
                    prepareRow();
                    syncWriter->WriteRowUnsafe(row, key);
                    ++writtenRowCount;
                    setProgress();
                }

                syncWriter->Close();
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

private:
    TKeyColumns KeyColumns;
    TPartitionChunkSequenceReaderPtr Reader;
    TTableChunkSequenceWriterPtr Writer;

};

TAutoPtr<IJob> CreatePartitionSortJob(IJobHost* host)
{
    return new TPartitionSortJob(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
