#include "stdafx.h"

#include "merge_job.h"

#include "config.h"
#include "job_detail.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NVersionedTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TMergeJob
    : public TSimpleJobBase
{
public:
    explicit TMergeJob(IJobHost* host, bool parallelReader)
        : TSimpleJobBase(host)
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);

        TKeyColumns keyColumns;

        if (JobSpec_.HasExtension(TMergeJobSpecExt::merge_job_spec_ext)) {
            const auto& mergeJobSpec = JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext);
            keyColumns = FromProto<Stroka>(mergeJobSpec.key_columns());
            LOG_INFO("Ordered merge produces sorted output");
        }

        std::vector<TChunkSpec> chunkSpecs;
        for (const auto& inputSpec : SchedulerJobSpecExt_.input_specs()) {
            for (const auto& chunkSpec : inputSpec.chunks()) {
                chunkSpecs.push_back(chunkSpec);
            }
        }

        TotalRowCount_ = GetCumulativeRowCount(chunkSpecs);

        NameTable_ = TNameTable::FromKeyColumns(keyColumns);

        auto readerFactory = parallelReader
            ? CreateSchemalessParallelMultiChunkReader
            : CreateSchemalessSequentialMultiChunkReader;

        ReaderFactory_ = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
            YCHECK(!Reader_);
            Reader_ = readerFactory(
                config->JobIO->TableReader,
                New<TMultiChunkReaderOptions>(),
                host->GetClient(),
                host->GetBlockCache(),
                host->GetNodeDirectory(),
                std::move(chunkSpecs),
                nameTable,
                columnFilter,
                TKeyColumns(),
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
                host->GetClient(),
                transactionId,
                chunkListId,
                true); // Allow value reordering if key columns are present.
            return Writer_;
        };
    }

private:
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

IJobPtr CreateOrderedMergeJob(IJobHost* host)
{
    return New<TMergeJob>(host, false);
}

IJobPtr CreateUnorderedMergeJob(IJobHost* host)
{
    return New<TMergeJob>(host, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
