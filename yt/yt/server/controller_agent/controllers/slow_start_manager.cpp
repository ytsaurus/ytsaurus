#include "slow_start_manager.h"

#include <yt/yt/ytlib/scheduler/config.h>

#include <algorithm>
#include <cmath>

namespace NYT::NControllerAgent::NControllers {

using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSlowStartManager
    : public ISlowStartManager
{
public:
    // For persistence only.
    TSlowStartManager() = default;

    explicit TSlowStartManager(TSlowStartConfigPtr config)
        : Config_(std::move(config))
        , JobCountLimit_(Config_->StartingJobCount)
    { }

    void OnJobRegistered() override
    {
        ++RegisteredJobCount_;
    }

    void OnJobUnregistered(bool isCompleted) override
    {
        YT_VERIFY(RegisteredJobCount_ > 0);
        --RegisteredJobCount_;
        if (!isCompleted) {
            return;
        }

        ++CompletedJobCount_;
        if (CompletedJobCount_ < GetSampleJobCount()) {
            return;
        }

        JobCountLimit_ *= Config_->GrowthFactor;
    }

    i64 GetRemainingAvailableJobCount() const override
    {
        return std::max<i64>(0, JobCountLimit_ - RegisteredJobCount_ - CompletedJobCount_);
    }

    void BuildInfo(TFluentMap fluent) const override
    {
        fluent
            .Item("registered_job_count").Value(RegisteredJobCount_)
            .Item("completed_job_count").Value(CompletedJobCount_)
            .Item("job_count_limit").Value(JobCountLimit_)
            .Item("sample_job_count").Value(GetSampleJobCount())
            .Item("remaining_available_job_count").Value(GetRemainingAvailableJobCount());
    }

private:
    TSlowStartConfigPtr Config_;

    i64 RegisteredJobCount_ = 0;
    i64 CompletedJobCount_ = 0;

    i64 JobCountLimit_ = 0;

    i64 GetSampleJobCount() const
    {
        return std::ceil(JobCountLimit_ * Config_->SampleRatio);
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSlowStartManager, 0x51057a27);
};

void TSlowStartManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Config_);
    PHOENIX_REGISTER_FIELD(2, RegisteredJobCount_);
    PHOENIX_REGISTER_FIELD(3, CompletedJobCount_);
    PHOENIX_REGISTER_FIELD(4, JobCountLimit_);
}

PHOENIX_DEFINE_TYPE(TSlowStartManager);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISlowStartManager> CreateSlowStartManager(TSlowStartConfigPtr config)
{
    return std::make_unique<TSlowStartManager>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
