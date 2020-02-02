#include "job_spec_helper.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/chunk_client/proto/data_source.pb.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/job_proxy/private.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/misc/assert.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger& Logger = JobProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobSpecHelper
    : public IJobSpecHelper
{
public:
    explicit TJobSpecHelper(const TJobSpec& jobSpec)
        : JobSpec_(jobSpec)
    {
        const auto& schedulerJobSpecExt = jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        JobIOConfig_ = ConvertTo<TJobIOConfigPtr>(TYsonString(schedulerJobSpecExt.io_config()));
        InputNodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();
        InputNodeDirectory_->MergeFrom(schedulerJobSpecExt.input_node_directory());
        auto dataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(GetSchedulerJobSpecExt().extensions());
        if (dataSourceDirectoryExt) {
            YT_LOG_DEBUG("Data source directory extension received\n%v", dataSourceDirectoryExt->DebugString());
            DataSourceDirectory_ = FromProto<TDataSourceDirectoryPtr>(*dataSourceDirectoryExt);
        }
    }

    virtual NJobTrackerClient::EJobType GetJobType() const override
    {
        return EJobType(JobSpec_.type());
    }

    virtual const NJobTrackerClient::NProto::TJobSpec& GetJobSpec() const override
    {
        return JobSpec_;
    }

    virtual NScheduler::TJobIOConfigPtr GetJobIOConfig() const override
    {
        return JobIOConfig_;
    }

    virtual TNodeDirectoryPtr GetInputNodeDirectory() const override
    {
        return InputNodeDirectory_;
    }

    virtual const TSchedulerJobSpecExt& GetSchedulerJobSpecExt() const override
    {
        return JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    }

    virtual const TDataSourceDirectoryPtr& GetDataSourceDirectory() const override
    {
        return DataSourceDirectory_;
    }

    virtual int GetKeySwitchColumnCount() const override
    {
        switch (GetJobType()) {
            case NScheduler::EJobType::Map:
            case NScheduler::EJobType::OrderedMap:
            case NScheduler::EJobType::PartitionMap:
            case NScheduler::EJobType::Vanilla:
                return 0;

            case NScheduler::EJobType::JoinReduce:
            case NScheduler::EJobType::SortedReduce: {
                const auto& reduceJobSpecExt = JobSpec_.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                const auto reduceKeyColumnCount = reduceJobSpecExt.reduce_key_column_count();
                const auto foreignKeyColumnCount = reduceJobSpecExt.join_key_column_count();
                return foreignKeyColumnCount != 0 ? foreignKeyColumnCount : reduceKeyColumnCount;
            }

            case NScheduler::EJobType::ReduceCombiner:
            case NScheduler::EJobType::PartitionReduce:
                {
                    const auto& reduceJobSpecExt = JobSpec_.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                    return reduceJobSpecExt.reduce_key_column_count();
                }

            default:
                YT_ABORT();
        }
    }

    virtual bool IsReaderInterruptionSupported() const override
    {
        switch (GetJobType()) {
            case NScheduler::EJobType::Map:
            case NScheduler::EJobType::OrderedMap:
            case NScheduler::EJobType::SortedReduce:
            case NScheduler::EJobType::JoinReduce:
            case NScheduler::EJobType::ReduceCombiner:
            case NScheduler::EJobType::PartitionReduce:
            case NScheduler::EJobType::SortedMerge:
            case NScheduler::EJobType::OrderedMerge:
            case NScheduler::EJobType::UnorderedMerge:
                return true;
            default:
                return false;
        }
    }

private:
    TJobSpec JobSpec_;
    TJobIOConfigPtr JobIOConfig_;
    TNodeDirectoryPtr InputNodeDirectory_;
    TDataSourceDirectoryPtr DataSourceDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

IJobSpecHelperPtr CreateJobSpecHelper(const NJobTrackerClient::NProto::TJobSpec& jobSpec)
{
    return New<TJobSpecHelper>(jobSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
