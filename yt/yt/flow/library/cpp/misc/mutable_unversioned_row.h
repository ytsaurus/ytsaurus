#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! A mutable owning variant of TUnversionedRow.
class TMutableUnversionedOwningRow
{
public:
    TMutableUnversionedOwningRow() = default;
    TMutableUnversionedOwningRow(NTableClient::TUnversionedValueRange range);
    explicit TMutableUnversionedOwningRow(NTableClient::TUnversionedRow other);
    TMutableUnversionedOwningRow(const TMutableUnversionedOwningRow& other) = default;
    TMutableUnversionedOwningRow(TMutableUnversionedOwningRow&& other) = default;

    TMutableUnversionedOwningRow& operator=(const TMutableUnversionedOwningRow& other) = default;
    TMutableUnversionedOwningRow& operator=(TMutableUnversionedOwningRow&& other) = default;

    // Checks if row is initialized.
    explicit operator bool() const;

    // Returns view. Can be called on unitialized row.
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

    size_t GetSpaceUsed() const;

    // Makes row initialized.
    void Reserve(int count);

    // Makes row initialized. Unitialized row is considered as empty.
    void PushBack(const NTableClient::TUnversionedOwningValue& value);

    void Set(int index, const NTableClient::TUnversionedOwningValue& value);

    NTableClient::TUnversionedOwningValue GetOwning(int index) const;

private:
    TBlob RowData_{GetRefCountedTypeCookie<NTableClient::TOwningRowTag>()}; // TRowHeader plus TValue-s.
    std::vector<TSharedRangeHolderPtr> StringHolders_;                      // Holds string data.

private:
    void Init(NTableClient::TUnversionedValueRange range);

    NTableClient::TUnversionedRowHeader* GetHeader();
    const NTableClient::TUnversionedRowHeader* GetHeader() const;

    NTableClient::TUnversionedValue* Data();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
