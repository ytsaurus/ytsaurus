#include "stdafx.h"

#include "partition_map_job_io.h"
#include "job.h"
#include "user_job_io_detail.h"

#include <ytlib/table_client/partitioner.h>
#include <ytlib/table_client/partition_chunk_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/sync_reader.h>

#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <core/ytree/yson_string.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NScheduler;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////

typedef TOldMultiChunkSequentialWriter<TPartitionChunkWriterProvider> TWriter;

////////////////////////////////////////////////////////////////////

class TPartitionMapJobIO
    : public TUserJobIOBase
{
public:
    TPartitionMapJobIO(IJobHost* host)
        : TUserJobIOBase(host)
    { 
        const auto& jobSpec = Host_->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        Partitioner_ = CreateHashPartitioner(jobSpecExt.partition_count());
        KeyColumns_ = FromProto<Stroka>(jobSpecExt.key_columns());
    }

    virtual ISyncWriterUnsafePtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId) override
    {
        
        options->KeyColumns = KeyColumns_;

        auto provider = New<TPartitionChunkWriterProvider>(
            JobIOConfig_->TableWriter,
            options,
            Partitioner_.get());

        return CreateSyncWriter<TPartitionChunkWriterProvider>(New<TWriter>(
            JobIOConfig_->TableWriter,
            options,
            provider,
            Host_->GetMasterChannel(),
            transactionId,
            chunkListId));
    }

    virtual std::vector<ISyncReaderPtr> DoCreateReaders() override
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

private:
    std::unique_ptr<IPartitioner> Partitioner_;
    TKeyColumns KeyColumns_;

};

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreatePartitionMapJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TPartitionMapJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
