#include "partition_map_job_io.h"
#include "job.h"
#include "user_job_io_detail.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/partitioner.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NScheduler::NProto;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////

class TPartitionMapJobIO
    : public TUserJobIOBase
{
public:
    explicit TPartitionMapJobIO(IJobHostPtr host)
        : TUserJobIOBase(host)
    { }

    virtual void InterruptReader() override
    {
        THROW_ERROR_EXCEPTION("Interrupting is not supported for partition map jobs");
    }

    virtual std::vector<TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override
    {
        return std::vector<TDataSliceDescriptor>();
    }

    virtual void PopulateResult(TSchedulerJobResultExt* schedulerJobResult) override
    {
        // Don't call base class method, no need to fill boundary keys.

        YCHECK(Writers_.size() == 1);
        auto& writer = Writers_.front();
        ToProto(schedulerJobResult->mutable_output_chunk_specs(), writer->GetWrittenChunksMasterMeta());
    }

private:
    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        // Key columns for partitioner come from job spec extension.
        const TTableSchema& /* tableSchema */) override
    {
        const auto& jobSpec = Host_->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        auto partitioner = CreateHashPartitioner(
            jobSpecExt.partition_count(),
            jobSpecExt.reduce_key_column_count());
        auto keyColumns = FromProto<TKeyColumns>(jobSpecExt.sort_key_columns());

        auto nameTable = TNameTable::FromKeyColumns(keyColumns);
        nameTable->SetEnableColumnNameValidation();

        // We pass partitioning columns through schema but input stream is not sorted.
        options->ValidateSorted = false;

        return CreatePartitionMultiChunkWriter(
            config,
            options,
            nameTable,
            TTableSchema::FromKeyColumns(keyColumns),
            Host_->GetClient(),
            CellTagFromId(chunkListId),
            transactionId,
            chunkListId,
            std::move(partitioner));
    }

    virtual ISchemalessMultiChunkReaderPtr DoCreateReader(
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        // NB(psushin): don't use parallel readers here to minimize nondetermenistics
        // behaviour in mapper, that may lead to huge problems in presence of lost jobs.
        return CreateRegularReader(false, std::move(nameTable), columnFilter);
    }

};

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreatePartitionMapJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionMapJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
