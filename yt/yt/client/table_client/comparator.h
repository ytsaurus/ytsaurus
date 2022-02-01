#pragma once

#include "public.h"

#include "key.h"

#include <yt/yt/library/codegen/function.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESortOrder,
    ((Ascending)   (0))
    ((Descending)  (1))
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

    //! Test if key #key belongs to the ray defined by #keyBound.
    bool TestKey(const TKey& key, const TKeyBound& keyBound) const;

    //! Compare key bounds according to their logical position on a line of all possible keys.
    //! If lhs and rhs belong to the same point, compare lower limit against upper limit as
    //! defined by #lowerVsUpperResult (i.e. if 0, lower == upper; if < 0, lower < upper; if > 0, lower > upper) .
    int CompareKeyBounds(const TKeyBound& lhs, const TKeyBound& rhs, int lowerVsUpperResult = 0) const;

    //! Compare two values belonging to the index #index of the key.
    int CompareValues(int index, const TUnversionedValue& lhs, const TUnversionedValue& rhs) const;

    //! Compare keys.
    int CompareKeys(const TKey& lhs, const TKey& rhs) const;

    //! Returns the strongest of two key bounds. Key bounds should be of same direction
    //! (but possibly of different inclusiveness).
    TKeyBound StrongerKeyBound(const TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Shorthand for #lhs = #StrongerKeyBound(#lhs, #rhs).
    void ReplaceIfStrongerKeyBound(TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Same as previous for owning key bounds.
    void ReplaceIfStrongerKeyBound(TOwningKeyBound& lhs, const TOwningKeyBound& rhs) const;

    //! Returns the weakest of two key bounds. Key bounds should be of same direction
    //! (but possibly of different inclusiveness).
    TKeyBound WeakerKeyBound(const TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Check if the range defined by two key bounds is empty.
    bool IsRangeEmpty(const TKeyBound& lowerBound, const TKeyBound& upperBound) const;

    //! Check if the range defined by two key bounds has empty interior, i.e. is empty or is a singleton key.
    bool IsInteriorEmpty(const TKeyBound& lowerBound, const TKeyBound& upperBound) const;

    //! Return length of the primary key to which this comparator corresponds.
    //! In particular, any key bound length passed as an argument must not exceed GetLength()
    //! and any key length should be equal to GetLength().
    int GetLength() const;

    //! If there exists such key K that #lhs == ">= K" and #rhs == "<= K", return it.
    std::optional<TKey> TryAsSingletonKey(const TKeyBound& lhs, const TKeyBound& rhs) const;

    //! Returns a comparator that compares rows by first #keyColumnCount columns and ignores other.
    TComparator Trim(int keyColumnCount) const;

    //! Returns true if at least one column has descending sort order.
    bool HasDescendingSortOrder() const;

    //! Empty comparator is identified with an absense of comparator.
    //! This may be used instead of TComparator.
    explicit operator bool() const;

private:
    void ValidateKey(const TKey& key) const;
    void ValidateKeyBound(const TKeyBound& keyBound) const;
};

void FormatValue(TStringBuilderBase* builder, const TComparator& comparator, TStringBuf spec);
TString ToString(const TComparator& comparator);

void Serialize(const TComparator& comparator, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

using TPrefixComparer = int(const TUnversionedValue*, const TUnversionedValue*, int);

int GetCompareSign(int value);

int CompareKeys(TRange<TUnversionedValue> lhs, TRange<TUnversionedValue> rhs, TPrefixComparer comparePrefix);

int CompareKeys(TLegacyKey lhs, TLegacyKey rhs, TPrefixComparer comparePrefix);

class TKeyComparer
    : public NCodegen::TCGFunction<TPrefixComparer>
{
public:
    using TBase = NCodegen::TCGFunction<TPrefixComparer>;
    using TBase::TBase;

    TKeyComparer(const TBase& base);
    TKeyComparer();
};

////////////////////////////////////////////////////////////////////////////////

TRange<TUnversionedValue> ToKeyRef(TUnversionedRow row);

TRange<TUnversionedValue> ToKeyRef(TUnversionedRow row, int prefix);

TRange<TUnversionedValue> ToKeyRef(TKey key);

////////////////////////////////////////////////////////////////////////////////

class TKeyBoundRef
    : public TRange<TUnversionedValue>
{
public:
    bool Inclusive;
    bool Upper;

    TKeyBoundRef(TRange<TUnversionedValue> base, bool inclusive = false, bool upper = false);
};

TKeyBoundRef MakeKeyBoundRef(const TKeyBound& bound);

TKeyBoundRef MakeKeyBoundRef(const TOwningKeyBound& bound);

TKeyBoundRef MakeKeyBoundRef(TUnversionedRow row, bool upper, int keyLength);

////////////////////////////////////////////////////////////////////////////////

int CompareWithWidening(
    TRange<TUnversionedValue> keyPrefix,
    TRange<TUnversionedValue> boundKey,
    TPrefixComparer comparePrefix);

int CompareWithWidening(
    TRange<TUnversionedValue> keyPrefix,
    TRange<TUnversionedValue> boundKey);

int TestKey(TRange<TUnversionedValue> key, const TKeyBoundRef& bound, TRange<ESortOrder> sortOrders);

int TestKeyWithWidening(TRange<TUnversionedValue> key, const TKeyBoundRef& bound);

int TestKeyWithWidening(TRange<TUnversionedValue> key, const TKeyBoundRef& bound, TPrefixComparer comparePrefix);

int TestKeyWithWidening(TRange<TUnversionedValue> key, const TKeyBoundRef& bound, TRange<ESortOrder> sortOrders);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, ESortOrder sortOrder, TStringBuf /* spec */);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
