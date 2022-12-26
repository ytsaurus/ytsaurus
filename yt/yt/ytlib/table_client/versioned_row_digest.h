#pragma once

#include "public.h"

#include <yt/yt/library/quantile_digest/public.h>

class TDigest;

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TVersionedRowDigest
{
    std::vector<i64> EarliestNthTimestamp;
    IQuantileDigestPtr LastTimestampDigest;
    IQuantileDigestPtr AllButLastTimestampDigest;
};

////////////////////////////////////////////////////////////////////////////////

struct IVersionedRowDigestBuilder
    : public TRefCounted
{
    virtual void OnRow(TVersionedRow row) = 0;

    virtual TVersionedRowDigest FlushDigest() = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedRowDigestBuilder)

////////////////////////////////////////////////////////////////////////////////

IVersionedRowDigestBuilderPtr CreateVersionedRowDigestBuilder(
    const TVersionedRowDigestConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TVersionedRowDigestExt* protoDigest, const TVersionedRowDigest& digest);
void FromProto(TVersionedRowDigest* digest, const NProto::TVersionedRowDigestExt& protoDigest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
