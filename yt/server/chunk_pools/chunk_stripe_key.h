#pragma once

#include "private.h"
#include "output_order.h"

#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TBoundaryKeys
{
    NTableClient::TKey MinKey;
    NTableClient::TKey MaxKey;

    void Persist(const TPersistenceContext& context);

    operator bool() const;
};

//! A generic key that allows us to sort chunk stripes.
class TChunkStripeKey
{
public:
    TChunkStripeKey(int index);
    TChunkStripeKey(TBoundaryKeys boundaryKeys);
    TChunkStripeKey(NChunkPools::TOutputOrder::TEntry entry);
    //! Used only for persistence.
    TChunkStripeKey();

    bool IsIndex() const;
    bool IsBoundaryKeys() const;
    bool IsOutputOrderEntry() const;

    operator bool() const;

    int& AsIndex();
    const int& AsIndex() const;

    TBoundaryKeys& AsBoundaryKeys();
    const TBoundaryKeys& AsBoundaryKeys() const;

    NChunkPools::TOutputOrder::TEntry& AsOutputOrderEntry();
    const NChunkPools::TOutputOrder::TEntry& AsOutputOrderEntry() const;

    void Persist(const TPersistenceContext& context);

private:
    TVariant<int, TBoundaryKeys, NChunkPools::TOutputOrder::TEntry> Key_;
};

TString ToString(const TChunkStripeKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
