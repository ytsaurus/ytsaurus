#pragma once

#include "../base/uuid.h"
#include "column.h"
#include "numeric.h"

namespace clickhouse {


/**
 * Represents a UUID column.
 */
class ColumnUUID : public Column {
public:
    ColumnUUID();

    explicit ColumnUUID(ColumnRef data);

    /// Appends one element to the end of column.
    void Append(const UUID& value);

    /// Returns element at given row number.
    const UUID At(size_t n) const;

    /// Returns element at given row number.
    inline const UUID operator [] (size_t n) const { return At(n); }

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

    ItemView GetItem(size_t) const override;

private:
    std::shared_ptr<ColumnUInt64> data_;
};

}
