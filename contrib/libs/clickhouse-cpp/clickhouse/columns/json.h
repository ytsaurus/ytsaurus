#pragma once

#include "column.h"
#include "string.h"
#include "nullable.h"

namespace clickhouse {

/**
 * JSON Column: Represents JSON values as strings.
 * Works only when ClickHouse outputs JSON as strings and requires the setting
 * output_format_native_write_json_as_string to be set to 1 for selecting data.
 * Inserting JSON data does not require setting this setting.
 * 
 * WARNING: THIS IS AN EXPERIMENTAL IMPLEMENTATION.
 * The API may change in the future as we continue working on full support for JSON columns.
 *
 * ClickHouse does not accept empty strings as JSON; it requires an empty object ({}).
 * For nullable columns, each row marked a NULL must contain {}.
 * For convenience `clickhouse::ColumnNullableT<ColumnJSON>` automatically inserts {} for NULL rows.
 */
class ColumnJSON : public Column {
public:

    ColumnJSON();
    explicit ColumnJSON(std::vector<std::string> data);

    /// Appends one element to the column.
    void Append(std::string_view str);

    void Append(const char* str);
    void Append(std::string&& str);

    std::string_view At(size_t n) const;
    inline std::string_view operator [] (size_t n) const { return At(n); }

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream. Column types with prefixes must implement it.
    void SavePrefix(OutputStream* output) override;

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
    std::shared_ptr<ColumnString> data_;
};

template <>
inline void ColumnNullableT<ColumnJSON>::Append(std::optional<std::string_view> value) {
    ColumnNullable::Append(!value.has_value());
    if (value.has_value()) {
        typed_nested_data_->Append(*value);
    } else {
        typed_nested_data_->Append(std::string_view("{}"));
    }
}

}
