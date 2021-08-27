#include "job_size_tracker.h"

#include <limits>

namespace NYT::NChunkPools {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static constexpr double HysteresisFactor = 1.5;
static constexpr i64 SafeInf64 = std::numeric_limits<i64>::max() / 4;

//! Make sure all components of a vector belong to [-SafeInf64, SafeInf64].
void SafeClamp(TResourceVector& vector)
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        vector.Values[kind] = std::clamp(vector.Values[kind], -SafeInf64, SafeInf64);
    }
}

class TJobSizeTracker
    : public IJobSizeTracker
{
public:
    TJobSizeTracker(TResourceVector limitVector, TLogger logger)
        : LocalLimitVector_(limitVector)
        , HysteresizedLocalLimitVector_(limitVector * HysteresisFactor)
        , Logger(logger)
    {
        SafeClamp(LocalLimitVector_);
        SafeClamp(HysteresizedLocalLimitVector_);
        YT_LOG_TRACE(
            "Job size tracker instantiated (LocalLimitVector: %v, HysteresizedLocalLimitVector: %v)",
            LocalLimitVector_,
            HysteresizedLocalLimitVector_);
        SwitchDominantResource(EResourceKind::DataWeight);
        CumulativeLimitVector_ = LocalLimitVector_;
        (void)InputSliceDataWeight_;
    }

    void AccountSlice(TResourceVector vector) override
    {
        LocalVector_ += vector;
        CumulativeVector_ += vector;
        YT_LOG_TRACE(
            "Slice accounted (LocalVector: %v, CumulativeVector: %v)",
            LocalVector_,
            CumulativeVector_);
    }

    double SuggestRowSplitFraction(TResourceVector vector) override
    {
        auto combinedGap = GetCombinedGap();

        if (combinedGap.HasNegativeComponent()) {
            return 0.0;
        }

        double fraction = 1.0;
        for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
            fraction = std::min(fraction, static_cast<double>(combinedGap.Values[kind]) / vector.Values[kind]);
        }

        return fraction;
    }

    struct TOverflowToken
    {
        EResourceKind OverflownResource;
        bool IsLocal;
    };

    std::optional<std::any> CheckOverflow(TResourceVector extraVector) override
    {
        std::optional<TOverflowToken> result;
        if (auto localViolatedResources = extraVector.ViolatedResources(GetLocalGap()); !localViolatedResources.empty()) {
            if (localViolatedResources.contains(DominantResource_)) {
                result = TOverflowToken{
                    .OverflownResource = DominantResource_,
                    .IsLocal = true,
                };
            } else {
                result = TOverflowToken{
                    .OverflownResource = *localViolatedResources.begin(),
                    .IsLocal = true,
                };
            }
        } else if (auto cumulativeViolatedResources = extraVector.ViolatedResources(GetCumulativeGap()); !cumulativeViolatedResources.empty()) {
            if (cumulativeViolatedResources.contains(DominantResource_)) {
                result = TOverflowToken{
                    .OverflownResource = DominantResource_,
                    .IsLocal = false,
                };
            } else {
                // This should normally be impossible, but still let's form a proper overflow token.
                result = TOverflowToken{
                    .OverflownResource = *cumulativeViolatedResources.begin(),
                    .IsLocal = false,
                };
            }
        }

        if (result) {
            YT_LOG_TRACE(
                "Overflow detected (ExtraVector: %v, LocalVector: %v, CumulativeVector: %v, LocalGap: %v, "
                "CumulativeGap: %v, OverflowIsLocal: %v, OverflowResource: %v)",
                extraVector,
                LocalVector_,
                CumulativeVector_,
                GetLocalGap(),
                GetCumulativeGap(),
                result->IsLocal,
                result->OverflownResource);
            return *result;
        }

        return std::nullopt;
    }

    void Flush(std::optional<std::any> overflowToken) override
    {
        std::optional<TOverflowToken> typedToken;
        if (overflowToken) {
            typedToken = std::any_cast<TOverflowToken>(*overflowToken);
        }

        YT_LOG_TRACE("Flushing job size tracker (LocalVector: %v, OverflowToken: %v)", LocalVector_, *typedToken);

        if (!typedToken) {
            DropRun(DominantResource_);
            return;
        }

        if (!typedToken->IsLocal && typedToken->OverflownResource == DominantResource_) {
            LocalVector_ = TResourceVector::Zero();
            CumulativeLimitVector_ += LocalLimitVector_;
            SafeClamp(CumulativeLimitVector_);
        } else {
            DropRun(typedToken->OverflownResource);
        }
    }

private:
    TResourceVector CumulativeVector_ = TResourceVector::Zero();
    TResourceVector CumulativeLimitVector_;
    TResourceVector LocalVector_ = TResourceVector::Zero();
    TResourceVector LocalLimitVector_;
    EResourceKind DominantResource_ = static_cast<EResourceKind>(-1);
    TResourceVector HysteresizedLocalLimitVector_;
    i64 InputSliceDataWeight_;

    TLogger Logger;

    void DropRun(EResourceKind dominantResource)
    {
        CumulativeLimitVector_ = LocalLimitVector_;
        CumulativeVector_ = TResourceVector::Zero();
        LocalVector_ = TResourceVector::Zero();

        if (dominantResource != DominantResource_) {
            SwitchDominantResource(dominantResource);
        }

        YT_LOG_TRACE("Run dropped (DominantResource: %v, CumulativeLimitVector: %v)", DominantResource_, CumulativeLimitVector_);
    }

    void SwitchDominantResource(EResourceKind dominantResource)
    {
        auto oldDominantResource = DominantResource_;
        DominantResource_ = dominantResource;
        HysteresizedLocalLimitVector_ = LocalLimitVector_ * HysteresisFactor;
        HysteresizedLocalLimitVector_.Values[DominantResource_] = std::numeric_limits<i64>::max();
        SafeClamp(HysteresizedLocalLimitVector_);
        YT_LOG_DEBUG(
            "Switching dominant resource (Resource: %v -> %v, HysteresizedLocalLimitVector: %v)",
            oldDominantResource,
            DominantResource_,
            HysteresizedLocalLimitVector_);
    }

    TResourceVector GetLocalGap() const
    {
        return HysteresizedLocalLimitVector_ - LocalVector_;
    }

    TResourceVector GetCumulativeGap() const
    {
        return CumulativeLimitVector_ - CumulativeVector_;
    }

    TResourceVector GetCombinedGap() const
    {
        TResourceVector result;
        auto localGap = GetLocalGap();
        auto cumulativeGap = GetCumulativeGap();
        for (const auto& resourceKind : TEnumTraits<EResourceKind>::GetDomainValues()) {
            result.Values[resourceKind] = std::min(localGap.Values[resourceKind], cumulativeGap.Values[resourceKind]);
        }
        return result;
    }
};

TString ToString(const TJobSizeTracker::TOverflowToken& token)
{
    return Format("{R: %v, L: %v}", token.OverflownResource, token.IsLocal);
}

////////////////////////////////////////////////////////////////////////////////

IJobSizeTrackerPtr CreateJobSizeTracker(TResourceVector limitVector, const TLogger& logger)
{
    return New<TJobSizeTracker>(limitVector, logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
