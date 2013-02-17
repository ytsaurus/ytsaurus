#include "stdafx.h"
#include "partition_map_job_io.h"
#include "config.h"
#include "user_job_io.h"
#include "job.h"

#include <ytlib/table_client/partitioner.h>
#include <ytlib/table_client/partition_chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>

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

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

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

    virtual ISyncWriterPtr CreateTableOutput(
        int index) override
    {
        YCHECK(index == 0);

        LOG_DEBUG("Opening partitioned output");

        const auto& jobSpec = Host->GetJobSpec();
        auto transactionId = TTransactionId::FromProto(jobSpec.output_transaction_id());
        const auto& outputSpec = jobSpec.output_specs(0);
        auto account = outputSpec.account();
        auto chunkListId = TChunkListId::FromProto(outputSpec.chunk_list_id());
        Writer = New<TPartitionChunkSequenceWriter>(
            IOConfig->TableWriter,
            Host->GetMasterChannel(),
            transactionId,
            account,
            chunkListId,
            KeyColumns,
            ~Partitioner);

        auto syncWriter = CreateSyncWriter(Writer);
        syncWriter->Open();

        return syncWriter;
    }

    virtual void PopulateResult(NScheduler::NProto::TJobResult* result) override
    {
        auto* resultExt = result->MutableExtension(NScheduler::NProto::TPartitionJobResultExt::partition_job_result_ext);
        ToProto(resultExt->mutable_chunks(), Writer->GetWrittenChunks());
        PopulateUserJobResult(resultExt->mutable_mapper_result());
    }

private:
    TAutoPtr<IPartitioner> Partitioner;
    TKeyColumns KeyColumns;
    mutable TPartitionChunkSequenceWriterPtr Writer;

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
