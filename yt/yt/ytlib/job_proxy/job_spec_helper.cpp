#include "job_spec_helper.h"

#include "config.h"
#include "helpers.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/chunk_client/proto/data_source.pb.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/private.h>

#include <yt/yt/ytlib/table_client/config.h>

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
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = JobProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobSpecHelper
    : public IJobSpecHelper
{
public:
    TJobSpecHelper(const TJobSpec& jobSpec)
        : JobSpec_(jobSpec)
    {
        const auto& jobSpecExt = JobSpec_.GetExtension(TJobSpecExt::job_spec_ext);
        JobIOConfig_ = ConvertTo<TJobIOConfigPtr>(TYsonStringBuf(jobSpecExt.io_config()));
        if (jobSpecExt.has_testing_options()) {
            JobTestingOptions_ = ConvertTo<TJobTestingOptionsPtr>(TYsonStringBuf(jobSpecExt.testing_options()));
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

    const std::vector<TDataSliceDescriptor> UnpackDataSliceDescriptors() const override
    {
        std::vector<TDataSliceDescriptor> dataSliceDescriptors;
        for (const auto& inputSpec : GetJobSpecExt().input_table_specs()) {
            auto descriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
            dataSliceDescriptors.insert(dataSliceDescriptors.end(), descriptors.begin(), descriptors.end());
        }
        return dataSliceDescriptors;
    }

    TTableReaderOptionsPtr GetTableReaderOptions() const override
    {
        return ConvertTo<TTableReaderOptionsPtr>(TYsonStringBuf(GetJobSpecExt().table_reader_options()));
    }

    int GetKeySwitchColumnCount() const override
    {
        switch (GetJobType()) {
            case EJobType::Map:
            case EJobType::OrderedMap:
            case EJobType::PartitionMap:
            case EJobType::Vanilla:
                return 0;

            case EJobType::JoinReduce:
            case EJobType::SortedReduce: {
                const auto& reduceJobSpecExt = JobSpec_.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                const auto reduceKeyColumnCount = reduceJobSpecExt.reduce_key_column_count();
                const auto foreignKeyColumnCount = reduceJobSpecExt.join_key_column_count();
                return foreignKeyColumnCount != 0 ? foreignKeyColumnCount : reduceKeyColumnCount;
            }

            case EJobType::ReduceCombiner:
            case EJobType::PartitionReduce:
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
            case EJobType::Map:
            case EJobType::OrderedMap:
            case EJobType::PartitionMap:
            case EJobType::SortedReduce:
            case EJobType::JoinReduce:
            case EJobType::ReduceCombiner:
            case EJobType::PartitionReduce:
            case EJobType::SortedMerge:
            case EJobType::OrderedMerge:
            case EJobType::UnorderedMerge:
            case EJobType::Partition:
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
