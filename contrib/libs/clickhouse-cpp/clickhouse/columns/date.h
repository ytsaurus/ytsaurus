#pragma once

#include "decimal.h"
#include "numeric.h"

#include <ctime>

namespace clickhouse {

/** */
class ColumnDate : public Column {
public:
    using ValueType = std::time_t;

    ColumnDate();
    explicit ColumnDate(std::vector<uint16_t>&& data);

    /// Appends one element to the end of column.
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    std::time_t At(size_t n) const;
    inline std::time_t operator [] (size_t n) const { return At(n); }

    /// Do append data as is -- number of day in Unix epoch, no conversions performed.
    void AppendRaw(uint16_t value);
    uint16_t RawAt(size_t n) const;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Get Raw Vector Contents
    std::vector<uint16_t>& GetWritableData();

    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Returns the capacity of the column
    size_t Capacity() const;

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
    std::shared_ptr<ColumnUInt16> data_;
};


class ColumnDate32 : public Column {
public:
    using ValueType = std::time_t;

    ColumnDate32();
    explicit ColumnDate32(std::vector<int32_t>&& data);

    /// Appends one element to the end of column.
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    std::time_t At(size_t n) const;

    inline std::time_t operator [] (size_t n) const { return At(n); }

    /// Do append data as is -- number of day in Unix epoch (32bit signed), no conversions performed.
    void AppendRaw(int32_t value);
    int32_t RawAt(size_t n) const;

    /// Get Raw Vector Contents
    std::vector<int32_t>& GetWritableData();

    /// Returns the capacity of the column
    size_t Capacity() const;

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
    std::shared_ptr<ColumnInt32> data_;
};


/** DateTime64 supports date-time values (number of seconds since UNIX epoch), from 1970 up to 2130. */
class ColumnDateTime : public Column {
public:
    using ValueType = std::time_t;

    ColumnDateTime();
    explicit ColumnDateTime(std::vector<uint32_t>&& data);

    explicit ColumnDateTime(std::string timezone);
    ColumnDateTime(std::string timezone, std::vector<uint32_t>&& data);

    /// Appends one element to the end of column.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    std::time_t At(size_t n) const;
    inline std::time_t operator [] (size_t n) const { return At(n); }

    /// Append raw as UNIX epoch seconds in uint32
    void AppendRaw(uint32_t value);
    uint32_t RawAt(size_t n) const;

    /// Timezone associated with a data column.
    std::string Timezone() const;

    /// Get Raw Vector Contents
    std::vector<uint32_t>& GetWritableData();

    /// Returns the capacity of the column
    size_t Capacity() const;

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

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
    std::shared_ptr<ColumnUInt32> data_;
};


/** DateTime64 supports date-time values of arbitrary sub-second precision, from 1900 up to 2300. */
class ColumnDateTime64 : public Column {
public:
    using ValueType = Int64;

    explicit ColumnDateTime64(size_t precision);
    ColumnDateTime64(size_t precision, std::string timezone);

    /// Appends one element to the end of column.
    void Append(const Int64& value);
    // It is a bit controversial: users might expect it to parse string of ISO8601 or some other human-friendly format,
    // but current implementation parses it as fractional integer with decimal point, e.g. "123.456".
//    void Append(const std::string& value);

    /// Returns element at given row number.
    Int64 At(size_t n) const;

    inline Int64 operator[](size_t n) const { return At(n); }

    /// Timezone associated with a data column.
    std::string Timezone() const;

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

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

    size_t GetPrecision() const;

private:
    ColumnDateTime64(TypeRef type, std::shared_ptr<ColumnDecimal> data);

private:
    std::shared_ptr<ColumnDecimal> data_;
    const size_t precision_;
};

}
