#pragma once

#include "private.h"

#include "serialize.h"

#include <yt/ytlib/table_client/unversioned_row.h>

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
    //! Used only for persistence.
    TOutputChunkTreeKey();

    bool IsIndex() const;
    bool IsBoundaryKeys() const;

    int& AsIndex();
    const int& AsIndex() const;

    TBoundaryKeys& AsBoundaryKeys();
    const TBoundaryKeys& AsBoundaryKeys() const;

    void Persist(const TPersistenceContext& context);

private:
    TVariant<int, TBoundaryKeys> Key_;
};

Stroka ToString(const TOutputChunkTreeKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
