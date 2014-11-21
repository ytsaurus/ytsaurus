#include "stdafx.h"
#include "partition_map_job_io.h"
#include "config.h"
#include "user_job_io.h"
#include "job.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/table_client/partitioner.h>
#include <ytlib/table_client/partition_chunk_writer.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <core/ytree/yson_string.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/job.pb.h>

#include <server/transaction_server/public.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NTransactionServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

typedef NChunkClient::TOldMultiChunkSequentialWriter<TPartitionChunkWriterProvider> TWriter;

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
        const auto& jobSpec = host->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        Partitioner = CreateHashPartitioner(jobSpecExt.partition_count());
        KeyColumns = FromProto<Stroka>(jobSpecExt.key_columns());
    }

    virtual int GetOutputCount() const override
    {
        return 1;
    }

    virtual std::unique_ptr<TTableProducer> CreateTableInput(int index, IYsonConsumer* consumer) override
    {
        return DoCreateTableInput<TOldMultiChunkSequentialReader>(index, consumer);
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

        auto WriterProvider = New<TPartitionChunkWriterProvider>(
            IOConfig->TableWriter,
            options,
            Partitioner.get());

        Writer = CreateSyncWriter<TPartitionChunkWriterProvider>(New<TWriter>(
            IOConfig->TableWriter,
            options,
            WriterProvider,
            Host->GetMasterChannel(),
            transactionId,
            chunkListId));

        return Writer;
    }

    virtual void PopulateResult(TJobResult* result) override
    {
        auto* schedulerResultExt = result->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        Writer->GetNodeDirectory()->DumpTo(schedulerResultExt->mutable_node_directory());
        ToProto(schedulerResultExt->mutable_chunks(), Writer->GetWrittenChunks());
    }

    virtual TDataStatistics GetOutputDataStatistics() const override
    {
        if (WriterProvider) {
            return WriterProvider->GetDataStatistics();
        } else {
            return ZeroDataStatistics();
        }
    }

private:
    std::unique_ptr<IPartitioner> Partitioner;
    TPartitionChunkWriterProviderPtr WriterProvider;
    TKeyColumns KeyColumns;
    ISyncWriterUnsafePtr Writer;

};

std::unique_ptr<TUserJobIO> CreatePartitionMapJobIO(
    TJobIOConfigPtr ioConfig,
    IJobHost* host)
{
    return std::unique_ptr<TUserJobIO>(new TPartitionMapJobIO(ioConfig, host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
