#pragma once

#include "unversioned_row.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! This class represents a (contextually) schemaful comparable row. It behaves
//! similarly to TUnversionedRow.
class TKey
{
public:
    //! A special null key, use it instead of enclosing TKey in std::optional.
    TKey() = default;

    TKey(const TUnversionedValue* begin, int length);

    //! Returns true if key is non-null and false otherwise.
    operator bool() const;

    //! Construct from a given row and possibly key length and validate that row does not contain
    //! setntinels of types Min, Max and Bottom. If key length is not specified, row length will be used instead.
    static TKey FromRow(const TUnversionedRow& row, std::optional<int> length = std::nullopt);

    //! Same as above, but does not check that row does not contain sentinels.
    //! NB: in debug mode value type check is still performed, but results in YT_ABORT().
    static TKey FromRowUnchecked(const TUnversionedRow& row, std::optional<int> length = std::nullopt);

    //! Performs a deep copy of underlying values into owning row.
    TUnversionedOwningRow AsOwningRow() const;

    const TUnversionedValue& operator[](int index) const;

    int GetLength() const;

    //! Helpers for printing and hashing.
    const TUnversionedValue* Begin() const;
    const TUnversionedValue* End() const;

    void Persist(const TPersistenceContext& context);

private:
    const TUnversionedValue* Begin_ = nullptr;

    int Length_ = 0;

    static void ValidateValueTypes(
        const TUnversionedValue* begin,
        const TUnversionedValue* end);
};

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TKey& lhs, const TKey& rhs);
bool operator!=(const TKey& lhs, const TKey& rhs);

void FormatValue(TStringBuilderBase* builder, const TKey& key, TStringBuf format);
TString ToString(const TKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

//! A hasher for TKey.
template <>
struct THash<NYT::NTableClient::TKey>
{
    inline size_t operator()(const NYT::NTableClient::TKey& key) const
    {
        return GetHash(key.Begin(), key.End());
    }
};
