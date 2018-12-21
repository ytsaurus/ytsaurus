#include "job_spec_helper.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/misc/assert.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient;

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

    virtual const NScheduler::NProto::TSchedulerJobSpecExt& GetSchedulerJobSpecExt() const override
    {
        return JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
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
                Y_UNREACHABLE();
        }
    }

    virtual bool IsReaderInterruptionSupported() const
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
                return true;
            default:
                return false;
        }
    }

private:
    TJobSpec JobSpec_;
    TJobIOConfigPtr JobIOConfig_;
    TNodeDirectoryPtr InputNodeDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

IJobSpecHelperPtr CreateJobSpecHelper(const NJobTrackerClient::NProto::TJobSpec& jobSpec)
{
    return New<TJobSpecHelper>(jobSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
