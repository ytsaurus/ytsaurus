#pragma once

#include "private.h"

#include <yt/server/chunk_pools/output_order.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TBoundaryKeys
{
    NTableClient::TKey MinKey;
    NTableClient::TKey MaxKey;

    void Persist(const TPersistenceContext& context);
};

//! A generic key that allows us to sort output chunk trees.
class TOutputChunkTreeKey
{
public:
    TOutputChunkTreeKey(int index);
    TOutputChunkTreeKey(TBoundaryKeys boundaryKeys);
    TOutputChunkTreeKey(NChunkPools::TOutputOrder::TEntry entry);
    //! Used only for persistence.
    TOutputChunkTreeKey();

    bool IsIndex() const;
    bool IsBoundaryKeys() const;
    bool IsOutputOrderEntry() const;

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

TString ToString(const TOutputChunkTreeKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
