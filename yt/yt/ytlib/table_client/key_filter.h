#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/xor_filter/public.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

bool Contains(const TXorFilter& filter, TLegacyKey key);

////////////////////////////////////////////////////////////////////////////////

class IKeyFilterBuilder
    : public TRefCounted
{
public:
    virtual void AddKey(TFingerprint fingerprint) = 0;

    virtual std::vector<TSharedRef> SerializeBlocks(NProto::TSystemBlockMetaExt* systemBlockMetaExt) = 0;

    virtual void FlushBlock(TLegacyKey key, bool force) = 0;

    virtual NChunkClient::EBlockType GetBlockType() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IKeyFilterBuilder)

////////////////////////////////////////////////////////////////////////////////

IKeyFilterBuilderPtr CreateXorFilterBuilder(TKeyFilterWriterConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTableClient
