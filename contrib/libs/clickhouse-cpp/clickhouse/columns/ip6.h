#pragma once

#include "string.h"
#include <memory>

struct in6_addr;

namespace clickhouse {

class ColumnIPv6 : public Column {
public:
    using DataType = in6_addr;
    using ValueType = in6_addr;

    ColumnIPv6();
    /** Takes ownership of the data, expects ColumnFixedString.
     * Modifying memory pointed by `data` from outside is UB.
     *
     * TODO: deprecate and remove as it is too dangerous and error-prone.
     */
    explicit ColumnIPv6(ColumnRef data);

    /// Appends one element to the column.
    void Append(const std::string_view& str);

    void Append(const in6_addr* addr);
    void Append(const in6_addr& addr);

    /// Returns element at given row number.
    in6_addr At(size_t n) const;

    /// Returns element at given row number.
    in6_addr operator [] (size_t n) const;

    std::string AsString(size_t n) const;

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t index) const override;

private:
    std::shared_ptr<ColumnFixedString> data_;
};

}
