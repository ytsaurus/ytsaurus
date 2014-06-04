#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "partition_sort_job.h"
#include "small_key.h"

#include <core/misc/varint.h>
#include <core/misc/ref_counted_tracker.h>

#include <core/yson/lexer.h>

#include <ytlib/table_client/value.h>
#include <ytlib/table_client/partition_chunk_reader.h>
#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/table_client/sync_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;

using NVersionedTableClient::TKey;
using NTableClient::TRow;
using NTableClient::TTableWriterOptionsPtr;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

typedef TOldMultiChunkParallelReader<TPartitionChunkReader> TReader;
typedef TOldMultiChunkSequentialWriter<TTableChunkWriterProvider> TWriter;

////////////////////////////////////////////////////////////////////////////////

class TPartitionSortJob
    : public TJob
{
public:
    explicit TPartitionSortJob(IJobHost* host)
        : TJob(host)
        , JobSpec(host->GetJobSpec())
        , SchedulerJobSpecExt(JobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
        , SortJobSpecExt(JobSpec.GetExtension(TSortJobSpecExt::sort_job_spec_ext))
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt.input_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt.input_specs(0);

        YCHECK(SchedulerJobSpecExt.output_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt.output_specs(0);

        KeyColumns = FromProto<Stroka>(SortJobSpecExt.key_columns());

        std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());
        std::random_shuffle(chunks.begin(), chunks.end());

        auto provider = New<TPartitionChunkReaderProvider>(
            config->JobIO->TableReader,
            host->GetUncompressedBlockCache());

        Reader = New<TReader>(
            config->JobIO->TableReader,
            host->GetMasterChannel(),
            host->GetCompressedBlockCache(),
            host->GetNodeDirectory(),
            std::move(chunks),
            provider);

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->KeyColumns = KeyColumns;

        auto writerProvider = New<TTableChunkWriterProvider>(
            config->JobIO->TableWriter,
            options);

        Writer = New<TWriter>(
            config->JobIO->TableWriter,
            options,
            writerProvider,
            host->GetMasterChannel(),
            transactionId,
            chunkListId);
    }

    virtual TJobResult Run() override
    {
        auto host = Host.Lock();
        YCHECK(host);

        const auto& jobSpec = host->GetJobSpec();
        const auto& schedulerJobSpecExt = jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        PROFILE_TIMING ("/sort_time") {
            auto keyColumnCount = KeyColumns.size();

            std::vector<TSmallKeyPart> keyBuffer;
            std::vector<const char*> rowPtrBuffer;
            std::vector<ui32> rowIndexHeap;

            i64 estimatedRowCount = schedulerJobSpecExt.input_row_count();

            LOG_INFO("Initializing");
            {
                Sync(Reader.Get(), &TReader::AsyncOpen);

                keyBuffer.reserve(estimatedRowCount * keyColumnCount);
                rowPtrBuffer.reserve(estimatedRowCount);
                rowIndexHeap.reserve(estimatedRowCount);

                LOG_INFO("Estimated row count: %v", estimatedRowCount);
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

                NYson::TStatelessLexer lexer;
                const TReader::TFacade* facade;
                while ((facade = Reader->GetFacade()) != nullptr) {
                    // Push row pointer.
                    rowPtrBuffer.push_back(facade->GetRowPointer());
                    rowIndexHeap.push_back(rowIndexHeap.size());
                    YASSERT(rowIndexHeap.back() <= std::numeric_limits<ui32>::max());

                    // Push key.
                    keyBuffer.resize(keyBuffer.size() + keyColumnCount);
                    for (int i = 0; i < keyColumnCount; ++i) {
                        auto value = facade->ReadValue(KeyColumns[i]);
                        if (!value.IsNull()) {
                            auto& keyPart = keyBuffer[rowIndexHeap.back() * keyColumnCount + i];
                            SetSmallKeyPart(keyPart, value.ToStringBuf(), lexer);
                        }
                    }

                    // Push row index and readjust the heap.
                    std::push_heap(rowIndexHeap.begin(), rowIndexHeap.end(), comparer);

                    if (!isNetworkReleased && Reader->GetIsFetchingComplete()) {
                        host->ReleaseNetwork();
                        isNetworkReleased =  true;
                    }

                    if (!Reader->FetchNext()) {
                        Sync(Reader.Get(), &TReader::GetReadyEvent);
                    }
                }

                if (!isNetworkReleased) {
                    host->ReleaseNetwork();
                }
            }
            PROFILE_TIMING_CHECKPOINT("read");

            i64 totalRowCount = rowIndexHeap.size();
            LOG_INFO("Total row count: %v", totalRowCount);

            LOG_DEBUG(
                "RefCountedTracker: %v",
                TRefCountedTracker::Get()->GetDebugInfo(2));

            if (!schedulerJobSpecExt.is_approximate()) {
                YCHECK(totalRowCount == estimatedRowCount);
            }

            LOG_INFO("Writing");
            {
                auto syncWriter = CreateSyncWriter<TTableChunkWriterProvider>(Writer);

                TMemoryInput input;
                TRow row;

                struct TKeyMemoryPoolTag {};
                TChunkedMemoryPool keyMemoryPool { TKeyMemoryPoolTag() };

                auto key = TKey::Allocate(&keyMemoryPool, keyColumnCount);
                bool isRowReady = false;

                auto prepareRow = [&] () {
                    YASSERT(!rowIndexHeap.empty());

                    auto rowIndex = rowIndexHeap.back();
                    rowIndexHeap.pop_back();

                    // Prepare key.
                    for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                        auto& keyPart = keyBuffer[rowIndex * keyColumnCount + keyIndex];
                        key[keyIndex] = MakeKeyPart(keyPart);
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

                        auto* facade = Writer->GetCurrentWriter();
                        if (facade) {
                            if (SchedulerJobSpecExt.enable_sort_verification()) {
                                facade->WriteRow(row);
                            } else {
                                facade->WriteRowUnsafe(row, key);
                            }
                        } else {
                            break;
                        }

                        isRowReady = false;
                        ++writtenRowCount;

                        setProgress();

                    } while (heapEnd != rowIndexHeap.end());
                }

                YCHECK(isRowReady || rowIndexHeap.empty());

                auto writeRowVerified = [&] () {
                    try {
                        syncWriter->WriteRow(row);
                    } catch (const TErrorException& ex) {
                        LOG_FATAL_IF(
                            ex.Error().FindMatching(NTableClient::EErrorCode::SortOrderViolation),
                            "Sort order violation in sort job.");
                        throw;
                    }
                };

                if (isRowReady) {
                    if (SchedulerJobSpecExt.enable_sort_verification()) {
                        writeRowVerified();
                    } else {
                        syncWriter->WriteRowUnsafe(row, key);
                    }
                    ++writtenRowCount;
                }

                // Synchronously write the rest of the rows.
                while (!rowIndexHeap.empty()) {
                    prepareRow();
                    if (SchedulerJobSpecExt.enable_sort_verification()) {
                        writeRowVerified();
                    } else {
                        syncWriter->WriteRowUnsafe(row, key);
                    }
                    ++writtenRowCount;
                    setProgress();
                }

                syncWriter->Close();
            }
            PROFILE_TIMING_CHECKPOINT("write");

            LOG_INFO("Finalizing");
            {
                TJobResult result;
                ToProto(result.mutable_error(), TError());

                auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                Writer->GetNodeDirectory()->DumpTo(schedulerResultExt->mutable_node_directory());
                ToProto(schedulerResultExt->mutable_chunks(), Writer->GetWrittenChunks());

                return result;
            }
        }
    }

    virtual double GetProgress() const override
    {
        i64 total = SchedulerJobSpecExt.input_row_count();
        if (total == 0) {
            LOG_WARNING("GetProgress: empty total");
            return 0;
        } else {
            // Split progress evenly between reading and writing.
            double progress =
                0.5 * Reader->GetProvider()->GetRowIndex() / total +
                0.5 * Writer->GetProvider()->GetRowCount() / total;
            LOG_DEBUG("GetProgress: %lf", progress);
            return progress;
        }
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Reader->GetFailedChunkIds();
    }

    virtual TJobStatistics GetStatistics() const override
    {
        TJobStatistics result;
        result.set_time(GetElapsedTime().MilliSeconds());
        ToProto(result.mutable_input(), Reader->GetProvider()->GetDataStatistics());
        ToProto(result.add_output(), Writer->GetProvider()->GetDataStatistics());
        return result;
    }

private:
    const TJobSpec& JobSpec;
    const TSchedulerJobSpecExt& SchedulerJobSpecExt;
    const TSortJobSpecExt& SortJobSpecExt;

    TKeyColumns KeyColumns;

    TIntrusivePtr<TReader> Reader;
    TIntrusivePtr<TWriter> Writer;

};

IJobPtr CreatePartitionSortJob(IJobHost* host)
{
    return New<TPartitionSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
