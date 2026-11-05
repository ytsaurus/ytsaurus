#pragma once

#include "column.h"
#include "numeric.h"

#include "../types/types.h"

#include <vector>

namespace clickhouse {

class ColumnBool : public Column {
public:
    using ValueType = bool;

    ColumnBool();
    explicit ColumnBool(std::vector<uint8_t> data);

    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Appends one element to the end of column.
    void Append(bool value);

    /// Returns element at given row number.
    bool At(size_t n) const;

    /// Returns element at given row number.
    bool operator[](size_t n) const { return At(n); }

    /// Returns the capacity of the column
    size_t Capacity() const;

public:
    /// Appends content of given column to the end of current one.
    /// Accepts ColumnBool or ColumnUInt8.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data.
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

private:
    ColumnUInt8 data_;
};

}  // namespace clickhouse
