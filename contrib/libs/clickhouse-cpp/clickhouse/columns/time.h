#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

class ColumnTime : public Column {
public:
    using ValueType = int32_t;

    ColumnTime();
    explicit ColumnTime(std::vector<int32_t>&& data);

    /// Appends one element to the end of column.
    void Append(ValueType value);

    /// Returns element at given row number.
    ValueType At(size_t n) const;
    ValueType operator[](size_t n) const { return At(n); }

    /// Get Raw Vector Contents
    std::vector<int32_t>& GetWritableData();

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Returns the capacity of the column
    size_t Capacity() const;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Clear column data .
    void Clear() override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

private:
    ColumnTime(TypeRef type, std::shared_ptr<ColumnInt32> data);

private:
    std::shared_ptr<ColumnInt32> data_;
};

class ColumnTime64 : public Column {
public:
    using ValueType = int64_t;

    explicit ColumnTime64(size_t precision);
    ColumnTime64(size_t precision, std::vector<int64_t>&& data);

    /// Appends one element to the end of column.
    void Append(ValueType value);

    /// Returns element at given row number.
    ValueType At(size_t n) const;

    ValueType operator[](size_t n) const { return At(n); }

    /// Get Raw Vector Contents
    std::vector<int64_t>& GetWritableData();

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Returns the capacity of the column
    size_t Capacity() const;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Clear column data .
    void Clear() override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

    size_t GetPrecision() const { return precision_; };

private:
    ColumnTime64(TypeRef type, std::shared_ptr<ColumnInt64> data);

private:
    std::shared_ptr<ColumnInt64> data_;
    const size_t precision_;
};

}
