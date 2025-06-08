#pragma once

#include "private.h"
#include "output_order.h"

#include <yt/yt/client/table_client/key.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TBoundaryKeys
{
    NTableClient::TKey MinKey;
    NTableClient::TKey MaxKey;

    bool operator ==(const TBoundaryKeys& other) const;

    explicit operator bool() const;

    PHOENIX_DECLARE_TYPE(TBoundaryKeys, 0x36ad5371);
};

//! A generic key that allows us to sort chunk stripes.
class TChunkStripeKey
{
public:
    TChunkStripeKey();
    explicit TChunkStripeKey(TBoundaryKeys boundaryKeys);
    explicit TChunkStripeKey(NChunkPools::TOutputOrder::TEntry entry);

    bool IsBoundaryKeys() const;
    bool IsOutputOrderEntry() const;

    explicit operator bool() const;

    TBoundaryKeys& AsBoundaryKeys();
    const TBoundaryKeys& AsBoundaryKeys() const;

    NChunkPools::TOutputOrder::TEntry& AsOutputOrderEntry();
    const NChunkPools::TOutputOrder::TEntry& AsOutputOrderEntry() const;

    bool operator ==(const TChunkStripeKey& other) const;

private:
    struct TUninitialized
    {
        bool operator ==(const TUninitialized& uninitializedTag) const = default;
        inline void Persist(const auto& /*context*/)
        { }
    };

    std::variant<
        TUninitialized,
        TBoundaryKeys,
        // TODO(coteeq): TOutputCookie
        NChunkPools::TOutputOrder::TEntry
    > Key_;

    PHOENIX_DECLARE_TYPE(TChunkStripeKey, 0x60a2ecee);
};

void FormatValue(TStringBuilderBase* builder, const TChunkStripeKey& key, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
