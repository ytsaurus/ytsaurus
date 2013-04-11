#include "stdafx.h"
#include "partition_map_job_io.h"
#include "config.h"
#include "user_job_io.h"
#include "job.h"

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/table_client/partitioner.h>
#include <ytlib/table_client/partition_chunk_writer.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/ytree/yson_string.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/job.pb.h>

#include <server/transaction_server/public.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
typedef NChunkClient::TMultiChunkSequentialWriter<TPartitionChunkWriter> TWriter;

////////////////////////////////////////////////////////////////////

class TPartitionMapJobIO
    : public TUserJobIO
{
public:
    TPartitionMapJobIO(
        TJobIOConfigPtr config,
        IJobHost* host)
        : TUserJobIO(config, host)
    {
        const auto& jobSpec = Host->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        Partitioner = CreateHashPartitioner(jobSpecExt.partition_count());
        KeyColumns = FromProto<Stroka>(jobSpecExt.key_columns());
    }

    virtual int GetOutputCount() const override
    {
        return 1;
    }

    virtual ISyncWriterPtr CreateTableOutput(int index) override
    {
        YCHECK(index == 0);

        LOG_DEBUG("Opening partitioned output");

        const auto& jobSpec = Host->GetJobSpec();
        const auto& schedulerJobSpecExt = jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        auto transactionId = FromProto<TTransactionId>(schedulerJobSpecExt.output_transaction_id());
        const auto& outputSpec = schedulerJobSpecExt.output_specs(0);
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->KeyColumns = KeyColumns;

        auto writerProvider = New<TPartitionChunkWriterProvider>(
            IOConfig->TableWriter,
            options,
            ~Partitioner);

        Writer = CreateSyncWriter<TPartitionChunkWriter>(New<TWriter>(
            IOConfig->TableWriter,
            options,
            writerProvider,
            Host->GetMasterChannel(),
            transactionId,
            chunkListId));
        Writer->Open();

        return Writer;
    }

    virtual void PopulateResult(NScheduler::NProto::TJobResult* result) override
    {
        Writer->GetNodeDirectory()->DumpTo(result->mutable_node_directory());
        ToProto(result->mutable_chunks(), Writer->GetWrittenChunks());

        auto* resultExt = result->MutableExtension(NScheduler::NProto::TPartitionJobResultExt::partition_job_result_ext);
        PopulateUserJobResult(resultExt->mutable_mapper_result());
    }

private:
    TAutoPtr<IPartitioner> Partitioner;
    TKeyColumns KeyColumns;
    ISyncWriterUnsafePtr Writer;

};

TAutoPtr<TUserJobIO> CreatePartitionMapJobIO(
    TJobIOConfigPtr ioConfig,
    IJobHost* host)
{
    return new TPartitionMapJobIO(ioConfig, host);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
