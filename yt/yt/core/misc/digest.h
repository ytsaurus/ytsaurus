#pragma once

#include "public.h"

#include <yt/core/misc/phoenix.h>

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
    : public NPhoenix::IPersistent
{
    virtual void AddSample(double value) = 0;

    virtual double GetQuantile(double alpha) const = 0;

    virtual void Persist(const NPhoenix::TPersistenceContext& context) = 0;
};

std::unique_ptr<IDigest> CreateLogDigest(TLogDigestConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
