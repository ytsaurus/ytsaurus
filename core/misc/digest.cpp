#include "digest.h"
#include "config.h"

#include <yt/core/misc/phoenix.h>

namespace NYT {

using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

class TLogDigest
    : public IDigest
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TLogDigest(TLogDigestConfigPtr config)
        : Step_(1 + config->RelativePrecision)
        , LogStep_(log(Step_))
        , LowerBound_(config->LowerBound)
        , UpperBound_(config->UpperBound)
        , DefaultValue_(config->DefaultValue ? *config->DefaultValue : config->LowerBound)
        , BucketCount_(std::max(1, static_cast<int>(ceil(log(UpperBound_ / LowerBound_) / LogStep_))))
        , Buckets_(BucketCount_)
    { }

    TLogDigest() = default;

    virtual void AddSample(double value) override
    {
        double bucketId = log(value / LowerBound_) / LogStep_;
        if (std::isnan(bucketId) || bucketId < std::numeric_limits<i32>::min() || bucketId > std::numeric_limits<i32>::max()) {
            // Discard all incorrect values (those that are non-positive, too small or too large).
            return;
        }
        ++Buckets_[std::max(0, std::min(BucketCount_ - 1, static_cast<int>(bucketId)))];
        ++SampleCount_;
    }

    virtual double GetQuantile(double alpha) const override
    {
        if (SampleCount_ == 0) {
            return DefaultValue_;
        }
        double value = LowerBound_;
        i64 sum = 0;
        for (int index = 0; index < BucketCount_; ++index) {
            if (sum >= alpha * SampleCount_) {
                return value;
            }
            sum += Buckets_[index];
            value *= Step_;
        }
        return UpperBound_;
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, Step_);
        Persist(context, LogStep_);
        Persist(context, LowerBound_);
        Persist(context, UpperBound_);
        Persist(context, DefaultValue_);
        Persist(context, BucketCount_);
        Persist(context, SampleCount_);
        Persist(context, Buckets_);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TLogDigest, 0x42424243);

private:
    double Step_;
    double LogStep_;

    double LowerBound_;
    double UpperBound_;
    double DefaultValue_;

    int BucketCount_;

    i64 SampleCount_ = 0;

    std::vector<i64> Buckets_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TLogDigest);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IDigest> CreateLogDigest(TLogDigestConfigPtr config)
{
    return std::make_unique<TLogDigest>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
