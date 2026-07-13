#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NYson {

struct IYsonConsumer;
class TYsonPullParserCursor;

} // namespace NYT::NYson

namespace NYT::NYTree {

struct INode;

} // namespace NYT::NYTree

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCompactUnversionedOwningRowData);

////////////////////////////////////////////////////////////////////////////////

//! A compact owning variant of TUnversionedRow.
//! Object occupies exactly 8 bytes (one intrusive pointer) + single allocation ref-counts, row header, values, and string data.
class TCompactUnversionedOwningRow
{
public:
    TCompactUnversionedOwningRow() = default;

    explicit TCompactUnversionedOwningRow(const NTableClient::TUnversionedValueRange& range);
    explicit TCompactUnversionedOwningRow(const NTableClient::TUnversionedRow& other);
    explicit TCompactUnversionedOwningRow(const NTableClient::TUnversionedOwningRow& other);

    TCompactUnversionedOwningRow(const TCompactUnversionedOwningRow& other) = default;
    TCompactUnversionedOwningRow(TCompactUnversionedOwningRow&& other) = default;

    TCompactUnversionedOwningRow& operator=(const TCompactUnversionedOwningRow& other) = default;
    TCompactUnversionedOwningRow& operator=(TCompactUnversionedOwningRow&& other) = default;

    // Checks if row is initialized.
    explicit operator bool() const;

    // Returns view. Can be called on uninitialized row.
    operator NTableClient::TUnversionedRow() const;
    NTableClient::TUnversionedRow Get() const;

    const NTableClient::TUnversionedValue* Begin() const;
    const NTableClient::TUnversionedValue* End() const;

    // STL interop.
    const NTableClient::TUnversionedValue* begin() const;
    const NTableClient::TUnversionedValue* end() const;

    NTableClient::TUnversionedValueRange Elements() const;
    NTableClient::TUnversionedValueRange FirstNElements(int count) const;

    const NTableClient::TUnversionedValue& operator[](int index) const;

    int GetCount() const;

    //! Constructs a row by calling `functor(mutableRow)` where mutableRow points into
    //! pre-allocated storage for count values + stringDataSize bytes of string data.
    //! After the |functor| returns, string data referenced by values is copied into the
    //! tail of the allocation and pointers are patched. YT_VERIFY is used to check
    //! that actual string data does not exceed |stringDataSize|.
    template <class TFunctor>
    TCompactUnversionedOwningRow(int count, size_t stringDataSize, TFunctor&& functor);

    size_t GetSpaceUsed() const;

    // Prefetches the single scattered row allocation into cache.
    void Prefetch() const noexcept;

    // Comparison operators (lexicographic, same as TUnversionedRow).
    friend bool operator==(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs);
    friend bool operator<(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs);
    friend bool operator<=(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs);
    friend bool operator>(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs);
    friend bool operator>=(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs);

private:
    TCompactUnversionedOwningRowDataPtr Data_;

    void Init(const NTableClient::TUnversionedValueRange& range);

    // Allocates storage for count values + stringDataSize bytes of string data,
    // initializes the row header, and returns a mutable view into the storage.
    NTableClient::TMutableUnversionedRow Preallocate(int count, size_t stringDataSize);

    // Copies string data referenced by values into the tail of the allocation
    // and patches the Data.String pointers. Verifies that total string data
    // does not exceed |stringDataSize|.
    void CopyStringData(int count, size_t stringDataSize);

    const NTableClient::TUnversionedRowHeader* GetHeader() const;
};

static_assert(sizeof(TCompactUnversionedOwningRow) == sizeof(void*), "TCompactUnversionedOwningRow must contain exactly one pointer");

////////////////////////////////////////////////////////////////////////////////

//! Constructs a TCompactUnversionedOwningRow from a list of values.
//! Analogous to NTableClient::MakeUnversionedOwningRow.
//! Definition is in compact_unversioned_owning_row-inl.h (requires helpers.h).
template <class... Ts>
TCompactUnversionedOwningRow MakeCompactUnversionedOwningRow(Ts&&... values);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TCompactUnversionedOwningRow& row, TStringBuf format);

void Serialize(const TCompactUnversionedOwningRow& row, NYson::IYsonConsumer* consumer);
void Deserialize(TCompactUnversionedOwningRow& row, TIntrusivePtr<NYTree::INode> node);
void Deserialize(TCompactUnversionedOwningRow& row, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

void ToProto(TProtobufString* protoRow, const TCompactUnversionedOwningRow& row);
void FromProto(TCompactUnversionedOwningRow* row, const TProtobufString& protoRow);

////////////////////////////////////////////////////////////////////////////////

//! A more efficient, allocation-free alternative to the |ToProto|/|FromProto| pair above:
//! these serialize directly into / parse directly from a caller-provided contiguous buffer
//! (e.g. an RPC attachment), avoiding the intermediate protobuf string and row buffer.
//! The wire format is identical to |ToProto|/|SerializeToString|.

//! Returns an upper bound on the number of bytes |SerializeToBuffer| writes for |row|.
size_t GetWireByteSize(const TCompactUnversionedOwningRow& row);

//! Serializes |row| in the unversioned row wire format directly into |dst|.
//! The buffer must hold at least |GetWireByteSize(row)| bytes.
//! Returns the pointer past the written bytes.
char* SerializeToBuffer(char* dst, const TCompactUnversionedOwningRow& row);

//! Parses a row serialized by |SerializeToBuffer| from |[begin, end)| into |row|.
//! String data is copied into the row's own allocation, so the source buffer may be released
//! afterwards. Returns the pointer past the consumed bytes.
const char* DeserializeFromBuffer(const char* begin, const char* end, TCompactUnversionedOwningRow* row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

//! A hasher for TCompactUnversionedOwningRow.
template <>
struct THash<NYT::NFlow::TCompactUnversionedOwningRow>
{
    size_t operator()(const NYT::NFlow::TCompactUnversionedOwningRow& row) const;
};

#include "compact_unversioned_owning_row-inl.h"
