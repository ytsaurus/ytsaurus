#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IMinHashDigestBuilder
    : public TRefCounted
{
    virtual void OnRow(TVersionedRow row) = 0;

    virtual TSharedRef SerializeBlock(NProto::TSystemBlockMetaExt* systemBlockMetaExt) = 0;

    virtual NChunkClient::EBlockType GetBlockType() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMinHashDigestBuilder)

////////////////////////////////////////////////////////////////////////////////

IMinHashDigestBuilderPtr CreateMinHashDigestBuilder(
    TMinHashDigestConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
