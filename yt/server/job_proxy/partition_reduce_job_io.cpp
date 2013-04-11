#include "stdafx.h"
#include "partition_reduce_job_io.h"
#include "config.h"
#include "sorting_reader.h"
#include "user_job_io.h"
#include "job.h"

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////

class TPartitionReduceJobIO
    : public TUserJobIO
{
public:
    TPartitionReduceJobIO(
        TJobIOConfigPtr ioConfig,
        IJobHost* host)
        : TUserJobIO(ioConfig, host)
    { }

    TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index,
        NYson::IYsonConsumer* consumer) override
    {
        YCHECK(index == 0);

        const auto& jobSpec = Host->GetJobSpec();
        const auto& schedulerJobSpecExt = jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        YCHECK(schedulerJobSpecExt.input_specs_size() == 1);

        const auto& inputSpec = schedulerJobSpecExt.input_specs(0);
        std::vector<NTableClient::NProto::TInputChunk> chunks(
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());

        auto jobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<Stroka>(jobSpecExt.key_columns());

        auto reader = CreateSortingReader(
            IOConfig->TableReader,
            Host->GetMasterChannel(),
            Host->GetBlockCache(),
            Host->GetNodeDirectory(),
            keyColumns,
            BIND(&IJobHost::ReleaseNetwork, Host),
            std::move(chunks),
            schedulerJobSpecExt.input_row_count(),
            schedulerJobSpecExt.is_approximate());

        YCHECK(index == Inputs.size());

        // NB: put reader here before opening, for proper failed chunk generation.
        Inputs.push_back(reader);

        reader->Open();

        return new TTableProducer(reader, consumer);
    }

    virtual void PopulateResult(TJobResult* result) override
    {
        auto* resultExt = result->MutableExtension(NScheduler::NProto::TReduceJobResultExt::reduce_job_result_ext);
        PopulateUserJobResult(resultExt->mutable_reducer_result());
    }

};

TAutoPtr<TUserJobIO> CreatePartitionReduceJobIO(
    TJobIOConfigPtr ioConfig,
    IJobHost* host)
{
    return new TPartitionReduceJobIO(ioConfig, host);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
