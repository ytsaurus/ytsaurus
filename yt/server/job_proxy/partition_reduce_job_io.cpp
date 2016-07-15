#include "partition_reduce_job_io.h"
#include "config.h"
#include "job.h"
#include "user_job_io_detail.h"

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/schemaless_partition_sort_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NTransactionClient;

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////

class TPartitionReduceJobIO
    : public TUserJobIOBase
{
public:
    explicit TPartitionReduceJobIO(IJobHostPtr host)
        : TUserJobIOBase(host)
        , ReduceJobSpecExt_(Host_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext))
    { }

    virtual int GetKeySwitchColumnCount() const override
    {
        return ReduceJobSpecExt_.reduce_key_column_count();
    }

    virtual ISchemalessMultiChunkReaderPtr DoCreateReader(
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        YCHECK(nameTable->GetSize() == 0 && columnFilter.All);
        YCHECK(SchedulerJobSpec_.input_table_specs_size() == 1);

        const auto& inputSpec = SchedulerJobSpec_.input_table_specs(0);
        auto dataSliceDescriptors = FromProto<std::vector<TDataSliceDescriptor>>(inputSpec.data_slice_descriptors());

        const auto& reduceJobSpecExt = Host_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<TKeyColumns>(reduceJobSpecExt.key_columns());
        nameTable = TNameTable::FromKeyColumns(keyColumns);

        YCHECK(reduceJobSpecExt.has_partition_tag());

        return CreateSchemalessPartitionSortReader(
            JobIOConfig_->TableReader,
            Host_->GetClient(),
            Host_->GetBlockCache(),
            Host_->GetInputNodeDirectory(),
            keyColumns,
            nameTable,
            BIND(&IJobHost::ReleaseNetwork, Host_),
            std::move(dataSliceDescriptors),
            SchedulerJobSpec_.input_row_count(),
            SchedulerJobSpec_.is_approximate(),
            reduceJobSpecExt.partition_tag());
    }

    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TTableSchema& tableSchema) override
    {
        return CreateTableWriter(config, options, chunkListId, transactionId, tableSchema);
    }

    virtual void PopulateResult(TSchedulerJobResultExt* schedulerJobResult) override
    {
        TUserJobIOBase::PopulateResult(schedulerJobResult);

        auto& writer = Writers_.front();

        // Partition reduce may come as intermediate job (reduce-combiner),
        // so we return written chunks to scheduler.
        ToProto(schedulerJobResult->mutable_output_chunk_specs(), writer->GetWrittenChunksMasterMeta());
    }

    virtual void InterruptReader() override
    {
        if (Reader_) {
            Reader_->Interrupt();
        }
    }

    virtual std::vector<TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override
    {
        return Reader_->GetUnreadDataSliceDescriptors(TRange<TUnversionedRow>());
    }

private:
    const TReduceJobSpecExt& ReduceJobSpecExt_;

};

std::unique_ptr<IUserJobIO> CreatePartitionReduceJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionReduceJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
