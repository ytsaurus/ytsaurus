#include "stdafx.h"

#include "partition_map_job_io.h"
#include "job.h"
#include "user_job_io_detail.h"

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/partitioner.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/ytree/yson_string.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NScheduler;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////

class TPartitionMapJobIO
    : public TUserJobIOBase
{
public:
    TPartitionMapJobIO(IJobHost* host)
        : TUserJobIOBase(host)
    { }

    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        // Key columns for partitioner come from job spec extension.
        const TKeyColumns& /* keyColumns */) override
    {
        const auto& jobSpec = Host_->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        auto partitioner = CreateHashPartitioner(jobSpecExt.partition_count());
        auto keyColumns = FromProto<TKeyColumns>(jobSpecExt.key_columns());

        auto nameTable = TNameTable::FromKeyColumns(keyColumns);
        return CreatePartitionMultiChunkWriter(
            JobIOConfig_->NewTableWriter,
            options,
            nameTable,
            keyColumns,
            Host_->GetMasterChannel(),
            transactionId,
            chunkListId,
            std::move(partitioner));
    }

    virtual std::vector<ISchemalessMultiChunkReaderPtr> DoCreateReaders() override
    {
        // ToDo(psushin): don't use parallel readers here to minimize nondetermenistics
        // behaviour in mapper, that may lead to huge problems in presence of lost jobs.
        return CreateRegularReaders(false);
    }

    virtual void PopulateResult(TSchedulerJobResultExt* schedulerJobResult) override
    {
        // Don't call base class method, no need to fill boundary keys.

        YCHECK(Writers_.size() == 1);
        auto& writer = Writers_.front();
        writer->GetNodeDirectory()->DumpTo(schedulerJobResult->mutable_node_directory());
        ToProto(schedulerJobResult->mutable_chunks(), writer->GetWrittenChunks());
    }

};

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreatePartitionMapJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionMapJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
