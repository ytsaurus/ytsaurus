#include "job_spec_helper.h"

#include "config.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/chunk_client/proto/data_source.pb.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NControllerAgent::NProto;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobSpecHelper
    : public IJobSpecHelper
{
public:
    TJobSpecHelper(const TJobSpec& jobSpec)
        : JobSpec_(jobSpec)
    {
        const auto& jobSpecExt = JobSpec_.GetExtension(TJobSpecExt::job_spec_ext);
        JobIOConfig_ = ConvertTo<TJobIOConfigPtr>(TYsonString(jobSpecExt.io_config()));
        if (jobSpecExt.has_testing_options()) {
            JobTestingOptions_ = ConvertTo<TJobTestingOptionsPtr>(TYsonString(jobSpecExt.testing_options()));
        } else {
            JobTestingOptions_ = New<TJobTestingOptions>();
        }
        auto dataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(GetJobSpecExt().extensions());
        if (dataSourceDirectoryExt) {
            YT_LOG_DEBUG("Data source directory extension received\n%v", dataSourceDirectoryExt->DebugString());
            DataSourceDirectory_ = FromProto<TDataSourceDirectoryPtr>(*dataSourceDirectoryExt);
        }
    }

    NJobTrackerClient::EJobType GetJobType() const override
    {
        return EJobType(JobSpec_.type());
    }

    const NControllerAgent::NProto::TJobSpec& GetJobSpec() const override
    {
        return JobSpec_;
    }

    NScheduler::TJobIOConfigPtr GetJobIOConfig() const override
    {
        return JobIOConfig_;
    }

    TJobTestingOptionsPtr GetJobTestingOptions() const override
    {
        return JobTestingOptions_;
    }

    const TJobSpecExt& GetJobSpecExt() const override
    {
        return JobSpec_.GetExtension(TJobSpecExt::job_spec_ext);
    }

    const TDataSourceDirectoryPtr& GetDataSourceDirectory() const override
    {
        return DataSourceDirectory_;
    }

    int GetKeySwitchColumnCount() const override
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

    bool IsReaderInterruptionSupported() const override
    {
        switch (GetJobType()) {
            case NScheduler::EJobType::Map:
            case NScheduler::EJobType::OrderedMap:
            case NScheduler::EJobType::PartitionMap:
            case NScheduler::EJobType::SortedReduce:
            case NScheduler::EJobType::JoinReduce:
            case NScheduler::EJobType::ReduceCombiner:
            case NScheduler::EJobType::PartitionReduce:
            case NScheduler::EJobType::SortedMerge:
            case NScheduler::EJobType::OrderedMerge:
            case NScheduler::EJobType::UnorderedMerge:
            case NScheduler::EJobType::Partition:
                return true;
            default:
                return false;
        }
    }

private:
    const TJobSpec JobSpec_;

    TJobIOConfigPtr JobIOConfig_;
    TDataSourceDirectoryPtr DataSourceDirectory_;
    TJobTestingOptionsPtr JobTestingOptions_;
};

////////////////////////////////////////////////////////////////////////////////

IJobSpecHelperPtr CreateJobSpecHelper(const NControllerAgent::NProto::TJobSpec& jobSpec)
{
    return New<TJobSpecHelper>(jobSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
