#include "job_size_tracker.h"

#include <limits>

namespace NYT::NChunkPools {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

static constexpr double HysteresisFactor = 1.5;
static constexpr i64 SafeInf64 = std::numeric_limits<i64>::max() / 4;

//! Make sure all components of a vector belong to [-SafeInf64, SafeInf64].
[[nodiscard]] TResourceVector SafeClamp(TResourceVector vector)
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        vector.Values[kind] = std::clamp(vector.Values[kind], -SafeInf64, SafeInf64);
    }
    return vector;
}

////////////////////////////////////////////////////////////////////////////////

class TJobSizeTracker final
    : public IJobSizeTracker
{
public:
    TJobSizeTracker(TResourceVector limitVector, TJobSizeTrackerOptions options, TLogger logger)
        : Options_(std::move(options))
        , LocalLimitVector_(SafeClamp(limitVector))
        , HysteresizedLocalLimitVector_(SafeClamp(limitVector * HysteresisFactor))
        , CumulativeLimitVector_(LocalLimitVector_)
        , Logger(std::move(logger))
    {
        SwitchDominantResource(EResourceKind::DataWeight);
        YT_TLOG_DEBUG("Job size tracker instantiated")
            .With("LocalLimitVector", LocalLimitVector_)
            .With("HysteresizedLocalLimitVector", HysteresizedLocalLimitVector_)
            .With("CumulativeLimitVector", CumulativeLimitVector_);
    }

    void AccountSlice(TResourceVector vector) override
    {
        LocalVector_ += vector;
        CumulativeVector_ += vector;
        YT_TLOG_TRACE("Slice accounted")
            .With("LocalVector", LocalVector_)
            .With("CumulativeVector", CumulativeVector_);
    }

    double SuggestRowSplitFraction(TResourceVector vector) const override
    {
        auto combinedGap = GetCombinedGap();

        if (combinedGap.HasNegativeComponent()) {
            return 0.0;
        }

        double fraction = 1.0;
        for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
            // NB: std::min(1.0, NaN) is UB.
            if (vector.Values[kind] > 0.0) {
                fraction = std::min(fraction, static_cast<double>(combinedGap.Values[kind]) / vector.Values[kind]);
            }
        }

        return fraction;
    }

    struct TOverflowToken
    {
        EResourceKind OverflownResource;
        bool IsLocal;
        i64 FlushIndex;
    };

    std::optional<std::any> CheckOverflow(TResourceVector extraVector) const override
    {
        std::optional<TOverflowToken> result;
        if (auto localViolatedResources = extraVector.ViolatedResources(GetLocalGap()); !localViolatedResources.empty()) {
            YT_VERIFY(!localViolatedResources.contains(DominantResource_));
            result = TOverflowToken{
                .OverflownResource = *localViolatedResources.begin(),
                .IsLocal = true,
            };
        } else if (auto cumulativeViolatedResources = extraVector.ViolatedResources(GetCumulativeGap()); !cumulativeViolatedResources.empty()) {
            if (cumulativeViolatedResources.contains(DominantResource_)) {
                result = TOverflowToken{
                    .OverflownResource = DominantResource_,
                    .IsLocal = false,
                };
            } else {
                result = TOverflowToken{
                    .OverflownResource = *cumulativeViolatedResources.begin(),
                    .IsLocal = false,
                };
            }
        }

        if (result) {
            result->FlushIndex = FlushIndex_;

            YT_TLOG_TRACE("Overflow detected")
                .With("ExtraVector", extraVector)
                .With("LocalVector", LocalVector_)
                .With("CumulativeVector", CumulativeVector_)
                .With("LocalLimitVector", LocalLimitVector_)
                .With("HysteresizedLocalLimitVector", HysteresizedLocalLimitVector_)
                .With("CumulativeLimitVector", CumulativeLimitVector_)
                .With("LocalGap", GetLocalGap())
                .With("CumulativeGap", GetCumulativeGap())
                .With("OverflowIsLocal", result->IsLocal)
                .With("OverflowResource", result->OverflownResource)
                .With("FlushIndex", FlushIndex_);
            return *result;
        }

        return std::nullopt;
    }

    void Flush(const std::optional<std::any>& overflowToken) override
    {
        std::optional<TOverflowToken> typedToken;
        if (overflowToken) {
            typedToken = std::any_cast<TOverflowToken>(*overflowToken);
        }

        YT_TLOG_TRACE("Flushing job size tracker")
            .With("LocalVector", LocalVector_)
            .With("OverflowToken", typedToken.has_value() ? std::optional(FormatToken(*typedToken)) : std::nullopt)
            .With("FlushIndex", FlushIndex_);

        YT_VERIFY(!typedToken.has_value() || typedToken->FlushIndex == FlushIndex_);

        ++FlushIndex_;

        if (!typedToken) {
            StartNewRun(DominantResource_);
            return;
        }

        if (Options_.LimitProgressionRatio && LimitProgressionLength_ < Options_.LimitProgressionLength) {
            if (LimitProgressionOffset_ == Options_.LimitProgressionOffset) {
                ++LimitProgressionLength_;
                // This method also takes care of clamping.
                LocalLimitVector_.PartialMultiply(*Options_.LimitProgressionRatio, Options_.GeometricResources, /*clampValue*/ SafeInf64);
                HysteresizedLocalLimitVector_.PartialMultiply(*Options_.LimitProgressionRatio, Options_.GeometricResources, /*clampValue*/ SafeInf64);
                // NB(apollo1321): PartialMultiply should not change the dominant resource inf limit value.
                HysteresizedLocalLimitVector_.Values[DominantResource_] = std::numeric_limits<i64>::max();
            } else {
                ++LimitProgressionOffset_;
            }

            YT_TLOG_TRACE("Limit progression iteration")
                .WithFormat("LimitProgressionLength", "%v/%v", LimitProgressionLength_, Options_.LimitProgressionLength)
                .WithFormat("LimitProgressionOffset", "%v/%v", LimitProgressionOffset_, Options_.LimitProgressionOffset)
                .With("LocalLimitVector", LocalLimitVector_)
                .With("HysteresizedLocalLimitVector", HysteresizedLocalLimitVector_);
        }

        YT_VERIFY(!typedToken->IsLocal || typedToken->OverflownResource != DominantResource_);

        if (typedToken->OverflownResource == DominantResource_) {
            LocalVector_ = TResourceVector::Zero();
            CumulativeLimitVector_ = SafeClamp(CumulativeLimitVector_ + LocalLimitVector_);
        } else {
            StartNewRun(typedToken->OverflownResource);
        }
    }

private:
    const TJobSizeTrackerOptions Options_;

    // Limits.
    TResourceVector LocalLimitVector_;
    TResourceVector HysteresizedLocalLimitVector_;
    TResourceVector CumulativeLimitVector_;

    // Usage.
    TResourceVector CumulativeVector_ = TResourceVector::Zero();
    TResourceVector LocalVector_ = TResourceVector::Zero();

    EResourceKind DominantResource_ = static_cast<EResourceKind>(-1);

    int LimitProgressionLength_ = 1;
    int LimitProgressionOffset_ = 0;

    i64 FlushIndex_ = 0;

    TLogger Logger;

    static std::string FormatToken(const TJobSizeTracker::TOverflowToken& token)
    {
        return Format("{R: %v, L: %v, I: %v}", token.OverflownResource, token.IsLocal, token.FlushIndex);
    }

    void StartNewRun(EResourceKind dominantResource)
    {
        CumulativeLimitVector_ = LocalLimitVector_;
        CumulativeVector_ = TResourceVector::Zero();
        LocalVector_ = TResourceVector::Zero();

        if (dominantResource != DominantResource_) {
            SwitchDominantResource(dominantResource);
        }

        YT_TLOG_TRACE("New run started")
            .With("DominantResource", DominantResource_)
            .With("CumulativeLimitVector", CumulativeLimitVector_);
    }

    void SwitchDominantResource(EResourceKind dominantResource)
    {
        auto oldDominantResource = DominantResource_;
        DominantResource_ = dominantResource;
        HysteresizedLocalLimitVector_ = LocalLimitVector_ * HysteresisFactor;
        HysteresizedLocalLimitVector_.Values[DominantResource_] = std::numeric_limits<i64>::max();
        HysteresizedLocalLimitVector_ = SafeClamp(HysteresizedLocalLimitVector_);
        YT_TLOG_DEBUG("Switching dominant resource")
            .With("OldResource", oldDominantResource)
            .With("NewResource", DominantResource_)
            .With("HysteresizedLocalLimitVector", HysteresizedLocalLimitVector_);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::unique_ptr<IJobSizeTracker> CreateJobSizeTracker(
    TResourceVector limitVector,
    TJobSizeTrackerOptions options,
    TLogger logger)
{
    return std::make_unique<TJobSizeTracker>(limitVector, std::move(options), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
