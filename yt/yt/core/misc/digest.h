#pragma once

#include "public.h"

#include <yt/yt/core/misc/phoenix.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! This class holds a compact representation of a set of samples.
//! #IDigest::GetQuantile|(alpha)| returns a lower bound |X| such that the number
//! of samples less than |X| is no less than |alpha|.
// TODO(max42): add methods GetCDF(X) -> alpha (inverse to GetQuantile).
// TODO(max42): add support for serialization/deserialization.
// TODO(max42): implement Q-Digest (https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/quantile/QDigest.java)
// and T-Digest (https://github.com/tdunning/t-digest) algorithms and compare them with TLogDigest.
struct IDigest
{
    virtual ~IDigest() = default;

    virtual void AddSample(double value) = 0;

    virtual double GetQuantile(double alpha) const = 0;

    virtual void Reset() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistentDigest
    : public IDigest
    , public NPhoenix::IPersistent
{
    void Persist(const NPhoenix::TPersistenceContext& context) override = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IPersistentDigest> CreateLogDigest(TLogDigestConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Make IDigest TRefCounted for consistency and stylistic reasons.
std::shared_ptr<IDigest> CreateHistogramDigest(THistogramDigestConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
