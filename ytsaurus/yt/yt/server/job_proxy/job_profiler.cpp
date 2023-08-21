#include "job_profiler.h"

#include "private.h"

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/library/process/subprocess.h>

#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/external_pprof.h>
#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/symbolize.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

namespace NYT::NJobProxy {

using namespace NScheduler;
using namespace NJobAgent;
using namespace NYTProf;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProfiler
    : public IJobProfiler
{
public:
    explicit TJobProfiler(const NScheduler::NProto::TSchedulerJobSpecExt* schedulerJobSpecExt)
    {
        for (const auto& protoProfilerSpec : schedulerJobSpecExt->job_profilers()) {
            auto profilerSpec = New<TJobProfilerSpec>();
            FromProto(profilerSpec.Get(), protoProfilerSpec);
            InitializeProfiler(profilerSpec);
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
            SymbolizeProfile(&profile, JobProxyCpuProfilerSpec_);
            JobProxyCpuProfile_->Blob = SerializeProfile(profile);
        }

        if (JobProxyMemoryProfile_) {
            auto profile = ConvertAllocationProfile(std::move(*JobProxyMemoryProfilingToken_).Stop());
            SymbolizeProfile(&profile, JobProxyMemoryProfilerSpec_);
            JobProxyMemoryProfile_->Blob = SerializeProfile(profile);
        }

        if (JobProxyPeakMemoryProfile_) {
            auto profile = ReadHeapProfile(tcmalloc::ProfileType::kPeakHeap);
            SymbolizeProfile(&profile, JobProxyPeakMemoryProfilerSpec_);
            JobProxyPeakMemoryProfile_->Blob = SerializeProfile(profile);
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

    void ProfilePeakMemoryUsage() override
    {
        InitializeJobProxyPeakMemoryProfiler(/*spec*/ nullptr);
    }

    TJobProfilerSpecPtr GetUserJobProfilerSpec() const override
    {
        return UserJobProfilerSpec_;
    }

    IOutputStream* GetUserJobProfileOutput() const override
    {
        return UserJobProfileStream_.get();
    }

    std::vector<TJobProfile> GetProfiles() const override
    {
        std::vector<TJobProfile> profiles;
        profiles.reserve(4);

        if (JobProxyCpuProfile_) {
            profiles.push_back(std::move(*JobProxyCpuProfile_));
        }

        if (JobProxyMemoryProfile_) {
            profiles.push_back(std::move(*JobProxyMemoryProfile_));
        }

        if (JobProxyPeakMemoryProfile_) {
            profiles.push_back(std::move(*JobProxyPeakMemoryProfile_));
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
    TJobProfilerSpecPtr JobProxyCpuProfilerSpec_;

    // Job proxy memory profiler.
    std::optional<TJobProfile> JobProxyMemoryProfile_;
    std::optional<tcmalloc::MallocExtension::AllocationProfilingToken> JobProxyMemoryProfilingToken_;
    TJobProfilerSpecPtr JobProxyMemoryProfilerSpec_;

    // Job proxy peak memory profiler.
    std::atomic<bool> DumpPeakMemoryUsage_ = false;
    std::optional<TJobProfile> JobProxyPeakMemoryProfile_;
    TJobProfilerSpecPtr JobProxyPeakMemoryProfilerSpec_;

    // User job profiler.
    std::optional<TJobProfile> UserJobProfile_;
    std::unique_ptr<TStringStream> UserJobProfileStream_;
    TJobProfilerSpecPtr UserJobProfilerSpec_;

    void InitializeProfiler(const TJobProfilerSpecPtr& spec)
    {
        if (spec->Binary == EProfilingBinary::JobProxy) {
            if (spec->Type == EProfilerType::Cpu) {
                InitializeJobProxyCpuProfiler(spec);
            } else if (spec->Type == EProfilerType::Memory) {
                InitializeJobProxyMemoryProfiler(spec);
            } else if (spec->Type == EProfilerType::PeakMemory) {
                InitializeJobProxyPeakMemoryProfiler(spec);
            }
        } else if (spec->Binary == EProfilingBinary::UserJob) {
            InitializeUserJobProfiler(spec);
        }
    }

    void InitializeJobProxyCpuProfiler(TJobProfilerSpecPtr spec)
    {
        TCpuProfilerOptions options{
            .SamplingFrequency = spec->SamplingFrequency,
        };
        JobProxyCpuProfiler_ = std::make_unique<TCpuProfiler>(options);
        JobProxyCpuProfile_ = TJobProfile{
            .Type = GetProfileTypeString(spec),
            .ProfilingProbability = spec->ProfilingProbability,
        };
        JobProxyCpuProfilerSpec_ = std::move(spec);
    }

    void InitializeJobProxyMemoryProfiler(const TJobProfilerSpecPtr& spec)
    {
        JobProxyMemoryProfile_ = TJobProfile{
            .Type = GetProfileTypeString(spec),
            .ProfilingProbability = spec->ProfilingProbability,
        };
        JobProxyMemoryProfilerSpec_ = spec;
    }

    void InitializeUserJobProfiler(TJobProfilerSpecPtr spec)
    {
        if (spec->Type != EProfilerType::Cpu && spec->Type != EProfilerType::Memory) {
            return;
        }

        UserJobProfileStream_ = std::make_unique<TStringStream>();
        UserJobProfile_ = TJobProfile{
            .Type = GetProfileTypeString(spec),
            .ProfilingProbability = spec->ProfilingProbability,
        };
        UserJobProfilerSpec_ = std::move(spec);
    }

    void InitializeJobProxyPeakMemoryProfiler(TJobProfilerSpecPtr spec)
    {
        if (!spec) {
            spec = New<TJobProfilerSpec>();
            spec->Binary = EProfilingBinary::JobProxy;
            spec->Type = EProfilerType::PeakMemory;
        }

        if (!DumpPeakMemoryUsage_.exchange(true)) {
            TJobProfile profile{
                .Type = GetProfileTypeString(spec),
                .ProfilingProbability = spec->ProfilingProbability,
            };
            JobProxyPeakMemoryProfile_ = profile;
            JobProxyPeakMemoryProfilerSpec_ = std::move(spec);
        }
    }

    void SymbolizeProfile(
        NYTProf::NProto::Profile* profile,
        const TJobProfilerSpecPtr& spec)
    {
        Symbolize(profile, /*filesOnly*/ true);
        AddBuildInfo(profile, TBuildInfo::GetDefault());

        if (spec->RunExternalSymbolizer) {
            SymbolizeByExternalPProf(profile, TSymbolizationOptions{
                .RunTool = RunSubprocess,
            });
        }
    }

    static TString GetProfileTypeString(const TJobProfilerSpecPtr& spec)
    {
        return Format("%lv_%lv", spec->Binary, spec->Type);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobProfiler> CreateJobProfiler(
    const NScheduler::NProto::TSchedulerJobSpecExt* schedulerJobSpecExt)
{
    return std::make_unique<TJobProfiler>(schedulerJobSpecExt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
