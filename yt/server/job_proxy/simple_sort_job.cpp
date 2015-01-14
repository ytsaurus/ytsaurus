#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "simple_sort_job.h"
#include "small_key.h"

#include <core/misc/sync.h>

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>
#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/yson/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;

using NVersionedTableClient::TKey;
using NTableClient::TRow;
using NTableClient::TTableWriterOptionsPtr;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

typedef TOldMultiChunkParallelReader<TTableChunkReader> TReader;
typedef TOldMultiChunkSequentialWriter<TTableChunkWriterProvider> TWriter;

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): get rid of value count.

class TSimpleSortJob
    : public TJob
{
public:
    explicit TSimpleSortJob(IJobHost* host)
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

        {
            auto options = New<TChunkReaderOptions>();
            options->KeepBlocks = true;

            std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());
            std::random_shuffle(chunks.begin(), chunks.end());

            auto provider = New<TTableChunkReaderProvider>(
                chunks,
                config->JobIO->TableReader,
                host->GetUncompressedBlockCache(),
                options);

            Reader = New<TReader>(
                config->JobIO->TableReader,
                host->GetMasterChannel(),
                host->GetCompressedBlockCache(),
                host->GetNodeDirectory(),
                std::move(chunks),
                provider);
        }

        {
            auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt.output_transaction_id());
            auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
            auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
            options->KeyColumns = KeyColumns;

            auto writerProvider = New<TTableChunkWriterProvider>(
                config->JobIO->TableWriter,
                options);

            Writer = CreateSyncWriter<TTableChunkWriterProvider>(New<TWriter>(
                config->JobIO->TableWriter,
                options,
                writerProvider,
                host->GetMasterChannel(),
                transactionId,
                chunkListId));
        }
    }

    virtual TJobResult Run() override
    {
        PROFILE_TIMING ("/sort_time") {
            size_t keyColumnCount = KeyColumns.size();

            yhash_map<TStringBuf, int> keyColumnToIndex;

            std::vector< std::pair<TStringBuf, TStringBuf> > valueBuffer;
            std::vector<TSmallKeyPart> keyBuffer;
            std::vector<ui32> valueIndexBuffer;
            std::vector<ui32> rowIndexBuffer;

            auto estimatedRowCount = SchedulerJobSpecExt.input_row_count();

            LOG_INFO("Initializing");
            {
                for (int i = 0; i < KeyColumns.size(); ++i) {
                    TStringBuf name(~KeyColumns[i], KeyColumns[i].size());
                    keyColumnToIndex[name] = i;
                }

                Sync(Reader.Get(), &TReader::AsyncOpen);

                valueBuffer.reserve(1000000);
                keyBuffer.reserve(estimatedRowCount * keyColumnCount);
                valueIndexBuffer.reserve(estimatedRowCount + 1);
                rowIndexBuffer.reserve(estimatedRowCount);

                // Add fake row.
                valueIndexBuffer.push_back(0);
            }

            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Reading");
            {
                NYson::TStatelessLexer lexer;
                const TReader::TFacade* facade;
                while ((facade = Reader->GetFacade()) != nullptr) {
                    rowIndexBuffer.push_back(rowIndexBuffer.size());
                    YASSERT(rowIndexBuffer.back() <= std::numeric_limits<ui32>::max());

                    keyBuffer.resize(keyBuffer.size() + keyColumnCount);

                    for (const auto& pair : facade->GetRow()) {
                        auto it = keyColumnToIndex.find(pair.first);
                        if (it != keyColumnToIndex.end()) {
                            auto& keyPart = keyBuffer[rowIndexBuffer.back() * keyColumnCount + it->second];
                            SetSmallKeyPart(keyPart, pair.second, lexer);
                        }
                        valueBuffer.push_back(pair);
                    }

                    valueIndexBuffer.push_back(valueBuffer.size());

                    if (!Reader->FetchNext()) {
                        Sync(Reader.Get(), &TReader::GetReadyEvent);
                    }
                }
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
            }
            );

            PROFILE_TIMING_CHECKPOINT("sort");

            LOG_INFO("Writing");
            {
                TRow row;

                struct TKeyMemoryPoolTag {};
                TChunkedMemoryPool keyMemoryPool { TKeyMemoryPoolTag() };

                auto key = TKey::Allocate(&keyMemoryPool, keyColumnCount);
                for (size_t progressIndex = 0; progressIndex < rowIndexBuffer.size(); ++progressIndex) {
                    row.clear();
                    ResetRowValues(&key);

                    ui32 rowIndex = rowIndexBuffer[progressIndex];
                    for (ui32 valueIndex = valueIndexBuffer[rowIndex];
                         valueIndex < valueIndexBuffer[rowIndex + 1];
                         ++valueIndex)
                    {
                        row.push_back(valueBuffer[valueIndex]);
                    }

                    for (int keyIndex = 0; keyIndex < keyColumnCount; ++keyIndex) {
                        auto& keyPart = keyBuffer[rowIndex * keyColumnCount + keyIndex];
                        key[keyIndex] = MakeKeyPart(keyPart);
                    }

                    if (SchedulerJobSpecExt.enable_sort_verification()) {
                        Writer->WriteRow(row);
                    } else {
                        Writer->WriteRowUnsafe(row, key);
                    }

                    if (progressIndex % 1000 == 0) {
                        Writer->SetProgress(double(progressIndex) / rowIndexBuffer.size());
                    }
                }

                Writer->Close();
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
                0.5 * Writer->GetRowCount() / total;
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
        ToProto(result.add_output(), Writer->GetDataStatistics());
        return result;
    }

private:
    const TJobSpec& JobSpec;
    const TSchedulerJobSpecExt& SchedulerJobSpecExt;
    const TSortJobSpecExt& SortJobSpecExt;

    TKeyColumns KeyColumns;

    TIntrusivePtr<TReader> Reader;
    ISyncWriterUnsafePtr Writer;

};

IJobPtr CreateSimpleSortJob(IJobHost* host)
{
    return New<TSimpleSortJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
