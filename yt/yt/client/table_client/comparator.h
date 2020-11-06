#pragma once

#include "public.h"

#include "key.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESortOrder,
    ((Ascending)   (0))
)

////////////////////////////////////////////////////////////////////////////////

//! Class that encapsulates all necessary information for key comparison
//! and testing if key belongs to the ray defined by a key bound.
class TComparator
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<ESortOrder>, SortOrders);

public:
    TComparator() = default;

    explicit TComparator(std::vector<ESortOrder> sortOrders);

    void Persist(const TPersistenceContext& context);

    bool TestKey(const TKey& key, const TKeyBound& keyBound) const;

    int GetLength() const;

private:
    void ValidateKey(const TKey& key) const;
    void ValidateKeyBound(const TKeyBound& keyBound) const;
};

void FormatValue(TStringBuilderBase* builder, const TComparator& comparator, TStringBuf spec);
TString ToString(const TComparator& comparator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
