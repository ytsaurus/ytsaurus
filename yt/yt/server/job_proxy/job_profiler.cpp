#include "job_profiler.h"

#include "private.h"

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/external_pprof.h>
#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/symbolize.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

namespace NYT::NJobProxy {

using namespace NScheduler::NProto;
using namespace NJobAgent;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf JobProxyCpuProfiler = "job_proxy_cpu";
constexpr TStringBuf JobProxyMemoryProfiler = "job_proxy_memory";
constexpr TStringBuf UserJobCpuProfiler = "user_job_cpu";
constexpr TStringBuf UserJobMemoryProfiler = "user_job_memory";

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProfiler
    : public IJobProfiler
{
public:
    explicit TJobProfiler(const TSchedulerJobSpecExt* schedulerJobSpecExt)
    {
        for (const auto& profilerSpec : schedulerJobSpecExt->job_profilers()) {
            InitializeProfiler(&profilerSpec);
        }
    }

    void Start() override
    {
        if (JobProxyCpuProfile_) {
            try {
                JobProxyCpuProfiler_->Start();
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to initialize job proxy CPU profiler");

                JobProxyCpuProfile_ = {};
                JobProxyCpuProfiler_ = {};
            }

            if (JobProxyCpuProfile_) {
                YT_LOG_INFO("Job proxy CPU profiler is enabled");
            }
        }

        if (JobProxyMemoryProfile_) {
            try {
                JobProxyMemoryProfilingToken_ = tcmalloc::MallocExtension::StartAllocationProfiling();
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to initialize job proxy memory profiler");

                JobProxyMemoryProfile_ = {};
            }

            if (JobProxyMemoryProfile_) {
                YT_LOG_INFO("Job proxy memory profiler is enabled");
            }
        }
    }

    void Stop() override
    {
        if (JobProxyCpuProfile_) {
            JobProxyCpuProfiler_->Stop();

            auto profile = JobProxyCpuProfiler_->ReadProfile();
            SymbolizeProfile(&profile);
            JobProxyCpuProfile_->Blob = SerializeProfile(profile);
        }

        if (JobProxyMemoryProfile_) {
            auto profile = ConvertAllocationProfile(std::move(*JobProxyMemoryProfilingToken_).Stop());
            SymbolizeProfile(&profile);
            JobProxyMemoryProfile_->Blob = SerializeProfile(profile);
        }

        if (UserJobProfile_) {
            auto profile = UserJobProfileStream_->Str();
            if (profile.empty()) {
                // User job did not send profile.
                UserJobProfile_ = {};
            } else {
                UserJobProfile_->Blob = std::move(profile);
            }
        }
    }

    std::optional<TString> GetUserJobProfilerName() const override
    {
        if (UserJobProfile_) {
            return UserJobProfile_->Type;
        } else {
            return {};
        }
    }

    IOutputStream* GetUserJobProfileOutput() const override
    {
        return UserJobProfileStream_.get();
    }

    std::vector<TJobProfile> GetProfiles() const override
    {
        std::vector<TJobProfile> profiles;
        profiles.reserve(3);

        if (JobProxyCpuProfile_) {
            profiles.push_back(std::move(*JobProxyCpuProfile_));
        }

        if (JobProxyMemoryProfile_) {
            profiles.push_back(std::move(*JobProxyMemoryProfile_));
        }

        if (UserJobProfile_) {
            profiles.push_back(std::move(*UserJobProfile_));
        }

        return profiles;
    }

private:
    // Job proxy CPU profiler.
    std::optional<TJobProfile> JobProxyCpuProfile_;
    std::unique_ptr<TCpuProfiler> JobProxyCpuProfiler_;

    // Job proxy memory profiler.
    std::optional<TJobProfile> JobProxyMemoryProfile_;
    std::optional<tcmalloc::MallocExtension::AllocationProfilingToken> JobProxyMemoryProfilingToken_;

    // User job profiler.
    std::optional<TJobProfile> UserJobProfile_;
    std::unique_ptr<TStringStream> UserJobProfileStream_;

    void InitializeProfiler(const TJobProfilerSpec* spec)
    {
        if (spec->profiler_name() == JobProxyCpuProfiler) {
            InitializeJobProxyCpuProfiler(spec);
        } else if (spec->profiler_name() == JobProxyMemoryProfiler) {
            InitializeJobProxyMemoryProfiler(spec);
        } else if (
            spec->profiler_name() == UserJobCpuProfiler ||
            spec->profiler_name() == UserJobMemoryProfiler)
        {
            InitializeUserJobProfiler(spec);
        }
    }

    void InitializeJobProxyCpuProfiler(const TJobProfilerSpec* spec)
    {
        YT_VERIFY(spec->profiler_name() == JobProxyCpuProfiler);

        JobProxyCpuProfiler_ = std::make_unique<TCpuProfiler>();
        JobProxyCpuProfile_ = TJobProfile{
            .Type = spec->profiler_name(),
            .ProfilingProbability = spec->profiling_probability(),
        };
    }

    void InitializeJobProxyMemoryProfiler(const TJobProfilerSpec* spec)
    {
        YT_VERIFY(spec->profiler_name() == JobProxyMemoryProfiler);

        JobProxyMemoryProfile_ = TJobProfile{
            .Type = spec->profiler_name(),
            .ProfilingProbability = spec->profiling_probability(),
        };
    }

    void InitializeUserJobProfiler(const TJobProfilerSpec* spec)
    {
        YT_VERIFY(
            spec->profiler_name() == UserJobCpuProfiler ||
            spec->profiler_name() == UserJobMemoryProfiler);

        UserJobProfileStream_ = std::make_unique<TStringStream>();
        UserJobProfile_ = TJobProfile{
            .Type = spec->profiler_name(),
            .ProfilingProbability = spec->profiling_probability(),
        };
    }

    void SymbolizeProfile(NYTProf::NProto::Profile* profile)
    {
        Symbolize(profile, /*filesOnly*/ true);
        AddBuildInfo(profile, TBuildInfo::GetDefault());

        SymbolizeByExternalPProf(profile, TSymbolizationOptions{
            .RunTool = RunSubprocess,
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobProfiler> CreateJobProfiler(const TSchedulerJobSpecExt* schedulerJobSpecExt)
{
    return std::make_unique<TJobProfiler>(schedulerJobSpecExt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
