#include "stdafx.h"
#include "partition_reduce_job_io.h"
#include "config.h"
#include "sorting_reader.h"
#include "job.h"

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>

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
        IJobHost* host,
        NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
        const NScheduler::NProto::TJobSpec& jobSpec)
        : TUserJobIO(ioConfig, mastersConfig, jobSpec)
        , Host(host)
    { }

    TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) override
    {
        YCHECK(index == 0);
        YCHECK(JobSpec.input_specs_size() == 1);

        std::vector<NTableClient::NProto::TInputChunk> chunks(
            JobSpec.input_specs(0).chunks().begin(),
            JobSpec.input_specs(0).chunks().end());

        auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

        auto jobSpecExt = JobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<Stroka>(jobSpecExt.key_columns());

        auto reader = CreateSortingReader(
            IOConfig->TableReader,
            MasterChannel,
            blockCache,
            keyColumns,
            BIND(&IJobHost::ReleaseNetwork, Host),
            MoveRV(chunks));
        reader->Open();

        // ToDo(psushin): init all inputs in constructor, get rid of this check.
        YCHECK(index == Inputs.size());
        Inputs.push_back(reader);

        return new TTableProducer(reader, consumer);
    }

    virtual void PopulateResult(TJobResult* result) override
    {
        auto* resultExt = result->MutableExtension(NScheduler::NProto::TReduceJobResultExt::reduce_job_result_ext);
        PopulateUserJobResult(resultExt->mutable_reducer_result());
    }

private:
    IJobHost* Host;

};

TAutoPtr<TUserJobIO> CreatePartitionReduceJobIO(
    TJobIOConfigPtr ioConfig,
    IJobHost* host,
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    return new TPartitionReduceJobIO(
        ioConfig,
        host,
        mastersConfig,
        jobSpec);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
