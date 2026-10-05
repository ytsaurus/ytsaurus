#pragma once

#include "array.h"
#include "column.h"
#include "numeric.h"
#include "tuple.h"

namespace clickhouse {

template <typename NestedColumnType, Type::Code type_code>
class ColumnGeo : public Column {
public:
    using ValueType = typename NestedColumnType::ValueType;

    ColumnGeo();

    explicit ColumnGeo(ColumnRef data);

    /// Appends one element to the end of column.
    template <typename T = ValueType>
    void Append(const T& value) {
        data_->Append(value);
    }

    /// Returns element at given row number.
    const ValueType At(size_t n) const;

    /// Returns element at given row number.
    inline const ValueType operator[](size_t n) const { return At(n); }

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

private:
    std::shared_ptr<NestedColumnType> data_;
};

// /**
//  * Represents a Point column.
//  */
using ColumnPoint = ColumnGeo<ColumnTupleT<ColumnFloat64, ColumnFloat64>, Type::Code::Point>;

/**
 * Represents a Ring column.
 */
using ColumnRing = ColumnGeo<ColumnArrayT<ColumnPoint>, Type::Code::Ring>;

/**
 * Represents a Polygon column.
 */
using ColumnPolygon = ColumnGeo<ColumnArrayT<ColumnRing>, Type::Code::Polygon>;

/**
 * Represents a MultiPolygon column.
 */
using ColumnMultiPolygon = ColumnGeo<ColumnArrayT<ColumnPolygon>, Type::Code::MultiPolygon>;

}  // namespace clickhouse
