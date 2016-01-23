#include "merge_job.h"
#include "private.h"
#include "config.h"
#include "job_detail.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TMergeJob
    : public TSimpleJobBase
{
public:
    TMergeJob(IJobHostPtr host, bool userParallelReader)
        : TSimpleJobBase(host)
        , UseParallelReader_(userParallelReader)
    {
        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);
    }

    virtual void Initialize() override
    {
        TKeyColumns keyColumns;
        TNullable<int> partitionTag;
        if (JobSpec_.HasExtension(TMergeJobSpecExt::merge_job_spec_ext)) {
            const auto& mergeJobSpec = JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext);
            keyColumns = FromProto<TKeyColumns>(mergeJobSpec.key_columns());
            if (mergeJobSpec.has_partition_tag()) {
                partitionTag = mergeJobSpec.partition_tag();
            }
            LOG_INFO("Ordered merge produces sorted output");
        }

        std::vector<TChunkSpec> chunkSpecs;
        for (const auto& inputSpec : SchedulerJobSpecExt_.input_specs()) {
            for (const auto& chunkSpec : inputSpec.chunks()) {
                chunkSpecs.push_back(chunkSpec);
            }
        }

        TotalRowCount_ = SchedulerJobSpecExt_.input_row_count();

        NameTable_ = TNameTable::FromKeyColumns(keyColumns);

        auto config = Host_->GetConfig();

        auto readerFactory = UseParallelReader_
            ? CreateSchemalessParallelMultiChunkReader
            : CreateSchemalessSequentialMultiChunkReader;

        ReaderFactory_ = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
            YCHECK(!Reader_);
            Reader_ = readerFactory(
                config->JobIO->TableReader,
                New<NTableClient::TTableReaderOptions>(),
                Host_->GetClient(),
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                std::move(chunkSpecs),
                nameTable,
                columnFilter,
                TKeyColumns(),
                partitionTag,
                NConcurrency::GetUnlimitedThrottler());
            return Reader_;
        };

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        const auto& outputSpec = SchedulerJobSpecExt_.output_specs(0);
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));

        WriterFactory_ = [=] (TNameTablePtr nameTable) {
            YCHECK(!Writer_);
            Writer_ = CreateSchemalessMultiChunkWriter(
                config->JobIO->TableWriter,
                options,
                nameTable,
                keyColumns,
                TOwningKey(),
                Host_->GetClient(),
                CellTagFromId(chunkListId),
                transactionId,
                chunkListId,
                true); // Allow value reordering if key columns are present.
            return Writer_;
        };
    }

private:
    const bool UseParallelReader_;

    TNameTablePtr NameTable_;


    virtual void CreateReader() override
    {
        ReaderFactory_(NameTable_, TColumnFilter());
    }

    virtual void CreateWriter() override
    {
        WriterFactory_(NameTable_);
    }
};

IJobPtr CreateOrderedMergeJob(IJobHostPtr host)
{
    return New<TMergeJob>(host, false);
}

IJobPtr CreateUnorderedMergeJob(IJobHostPtr host)
{
    return New<TMergeJob>(host, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
