#pragma once

#include "public.h"

#include <yt/yt/library/quantile_digest/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TVersionedRowDigest
    : public TRefCounted
{
    std::vector<i64> EarliestNthTimestamp;
    IQuantileDigestPtr LastTimestampDigest;
    IQuantileDigestPtr AllButLastTimestampDigest;
    IQuantileDigestPtr FirstTimestampDigest;
};

DEFINE_REFCOUNTED_TYPE(TVersionedRowDigest)

////////////////////////////////////////////////////////////////////////////////

struct IVersionedRowDigestBuilder
    : public TRefCounted
{
    virtual void OnRow(TVersionedRow row) = 0;

    virtual TVersionedRowDigestPtr FlushDigest() = 0;
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
