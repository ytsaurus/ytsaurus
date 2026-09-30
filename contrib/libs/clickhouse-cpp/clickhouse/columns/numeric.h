#pragma once

#include "column.h"
#include "absl/numeric/int128.h"

namespace clickhouse {

/**
 * Represents various numeric columns.
 */
template <typename T>
class ColumnVector : public Column {
public:
    using DataType = T;
    using ValueType = T;

    ColumnVector();

    explicit ColumnVector(const std::vector<T>& data);
    explicit ColumnVector(std::vector<T> && data);

    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Appends one element to the end of column.
    void Append(const T& value);

    /// Returns element at given row number.
    const T& At(size_t n) const;

    /// Returns element at given row number.
    inline const T& operator [] (size_t n) const { return At(n); }

    void Erase(size_t pos, size_t count = 1);

    /// Get Raw Vector Contents
    std::vector<T>& GetWritableData();

    /// Returns the capacity of the column
    size_t Capacity() const;

public:
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
    std::vector<T> data_;
};

using Int128 = absl::int128;
using UInt128 = absl::uint128;
using Int64 = int64_t;

using ColumnUInt8   = ColumnVector<uint8_t>;
using ColumnUInt16  = ColumnVector<uint16_t>;
using ColumnUInt32  = ColumnVector<uint32_t>;
using ColumnUInt64  = ColumnVector<uint64_t>;
using ColumnUInt128 = ColumnVector<UInt128>;

using ColumnInt8    = ColumnVector<int8_t>;
using ColumnInt16   = ColumnVector<int16_t>;
using ColumnInt32   = ColumnVector<int32_t>;
using ColumnInt64   = ColumnVector<int64_t>;
using ColumnInt128  = ColumnVector<Int128>;

using ColumnFloat32 = ColumnVector<float>;
using ColumnFloat64 = ColumnVector<double>;

}
