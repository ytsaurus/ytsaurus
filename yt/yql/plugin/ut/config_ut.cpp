#include <yt/yql/plugin/config.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NYqlPlugin {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDQYTBackendPtr ParseBackend(ui32 jobsPerOperation = 5, i32 workerCapacity = 24, i64 cpuLimit = 6)
{
    return ConvertTo<TDQYTBackendPtr>(TYsonString(Format(R"({
        cluster_name="local";
        jobs_per_operation=%v;
        max_jobs=10;
        vanilla_job_lite="/bin/dq_vanilla_job_lite";
        vanilla_job_command="./dq_vanilla_job";
        token_file="/tmp/token";
        worker_capacity=%v;
        cpu_limit=%v;
    })", jobsPerOperation, workerCapacity, cpuLimit)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDqConfigValidationTest, AcceptsConsistentJobCounts)
{
    EXPECT_NO_THROW(ParseBackend());
}

TEST(TDqConfigValidationTest, RejectsOperationLargerThanMaximum)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseBackend(11),
        "max_jobs (10) must not be less than jobs_per_operation (11)");
}

TEST(TDqConfigValidationTest, RejectsUnfillableMaximum)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseBackend(6),
        "max_jobs (10) must be divisible by jobs_per_operation (6)");
}

TEST(TDqConfigValidationTest, RejectsNonPositiveWorkerCapacity)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseBackend(5, 0),
        "worker_capacity must be positive");
}

TEST(TDqConfigValidationTest, AcceptsZeroCpuLimit)
{
    EXPECT_NO_THROW(ParseBackend(5, 24, 0));
}

TEST(TDqConfigValidationTest, AcceptsDuplicateVanillaJobFileNames)
{
    EXPECT_NO_THROW(ConvertTo<TDQYTBackendPtr>(TYsonString(TString(R"({
        cluster_name="local";
        jobs_per_operation=1;
        max_jobs=1;
        vanilla_job_lite="/bin/dq_vanilla_job_lite";
        token_file="/tmp/token";
        vanilla_job_file=[
            {name="lib.so";local_path="/first/lib.so";};
            {name="lib.so";local_path="/second/lib.so";};
        ];
    })"))));
}

TEST(TDqConfigValidationTest, AcceptsCacheLargerThanTmpfsMemoryLimit)
{
    EXPECT_NO_THROW(ConvertTo<TDQYTBackendPtr>(TYsonString(TString(R"({
        cluster_name="local";
        jobs_per_operation=1;
        max_jobs=1;
        vanilla_job_lite="/bin/dq_vanilla_job_lite";
        token_file="/tmp/token";
        use_tmp_fs=%true;
        memory_limit=1024;
        cache_size=2048;
    })"))));
}

TEST(TDqConfigValidationTest, AcceptsEnabledDqWithoutNativeBackends)
{
    EXPECT_NO_THROW(ConvertTo<TYqlPluginConfigPtr>(TYsonString(TString(R"({
        enable_dq=%true;
    })"))));
}

TEST(TDqConfigValidationTest, UsesExplicitNodeIdRange)
{
    EXPECT_NO_THROW(ConvertTo<TDQManagerConfigPtr>(TYsonString(TString(R"({
        interconnect_port=31002;
        grpc_port=31001;
        yt_coordinator={cluster_name="local";token_file="/tmp/token";};
        yt_backends=[{
            cluster_name="local";
            jobs_per_operation=5;
            max_jobs=20;
            vanilla_job_lite="/bin/dq_vanilla_job_lite";
            token_file="/tmp/token";
            min_node_id=512;
            max_node_id=532;
        }];
    })"))));

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TDQManagerConfigPtr>(TYsonString(TString(R"({
            interconnect_port=31002;
            grpc_port=31001;
            yt_coordinator={cluster_name="local";token_file="/tmp/token";};
            yt_backends=[{
                cluster_name="local";
                jobs_per_operation=5;
                max_jobs=20;
                vanilla_job_lite="/bin/dq_vanilla_job_lite";
                token_file="/tmp/token";
                min_node_id=512;
                max_node_id=531;
            }];
        })"))),
        "max_jobs (20) exceeds its worker node ID range (19)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYqlPlugin
