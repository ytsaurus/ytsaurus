#pragma once

#include "column.h"
#include "numeric.h"

#include <optional>

namespace clickhouse {

/**
 * Represents column of Nullable(T).
 */
class ColumnNullable : public Column {
public:
    ColumnNullable(ColumnRef nested, ColumnRef nulls);

    /// Appends one null flag to the end of the column
    void Append(bool isnull);

    /// Returns null flag at given row number.
    bool IsNull(size_t n) const;

    /// Returns nested column.
    ColumnRef Nested() const;

    /// Returns nulls column.
    ColumnRef Nulls() const;

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream.
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
    void Swap(Column&) override;

    ItemView GetItem(size_t) const override;

private:
    ColumnRef nested_;
    std::shared_ptr<ColumnUInt8> nulls_;
};

template <typename ColumnType>
class ColumnNullableT : public ColumnNullable {
public:
    using NestedColumnType = ColumnType;
    using ValueType = std::optional<std::decay_t<decltype(std::declval<NestedColumnType>().At(0))>>;

    ColumnNullableT(std::shared_ptr<NestedColumnType> data, std::shared_ptr<ColumnUInt8> nulls)
        : ColumnNullable(data, nulls)
        , typed_nested_data_(data)
    {}

    explicit ColumnNullableT(std::shared_ptr<NestedColumnType> data)
        : ColumnNullableT(data, FillNulls(data->Size()))
    {}

    template <typename ...Args>
    explicit ColumnNullableT(Args &&... args)
        : ColumnNullableT(std::make_shared<NestedColumnType>(std::forward<Args>(args)...))
    {}

    inline ValueType At(size_t index) const {
        return IsNull(index) ? ValueType{} : ValueType{typed_nested_data_->At(index)};
    }

    inline ValueType operator[](size_t index) const { return At(index); }

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override {
        ColumnNullable::Append(std::move(column));
    }

    inline void Append(ValueType value) {
        ColumnNullable::Append(!value.has_value());
        if (value.has_value()) {
            typed_nested_data_->Append(std::move(*value));
        } else {
            typed_nested_data_->Append(typename ValueType::value_type{});
        }
    }

    /** Create a ColumnNullableT from a ColumnNullable, without copying data and offsets, but by
     * 'stealing' those from `col`.
     *
     *  Ownership of column internals is transferred to returned object, original (argument) object
     *  MUST NOT BE USED IN ANY WAY, it is only safe to dispose it.
     *
     *  Throws an exception if `col` is of wrong type, it is safe to use original col in this case.
     *  This is a static method to make such conversion verbose.
     */
    static auto Wrap(ColumnNullable&& col) {
        return std::make_shared<ColumnNullableT<NestedColumnType>>(
            col.Nested()->AsStrict<NestedColumnType>(),
            col.Nulls()->AsStrict<ColumnUInt8>()) ;
    }

    static auto Wrap(Column&& col) { return Wrap(std::move(dynamic_cast<ColumnNullable&&>(col))); }

    // Helper to simplify integration with other APIs
    static auto Wrap(ColumnRef&& col) { return Wrap(std::move(*col->AsStrict<ColumnNullable>())); }

    ColumnRef Slice(size_t begin, size_t size) const override {
        return Wrap(ColumnNullable::Slice(begin, size));
    }

    ColumnRef CloneEmpty() const override { return Wrap(ColumnNullable::CloneEmpty()); }

    void Swap(Column& other) override {
        auto& col = dynamic_cast<ColumnNullableT<NestedColumnType>&>(other);
        typed_nested_data_.swap(col.typed_nested_data_);
        ColumnNullable::Swap(other);
    }

private:
    static inline auto FillNulls(size_t n){
        auto result = std::make_shared<ColumnUInt8>();
        for (size_t i = 0; i < n; ++i) {
            result->Append(0);
        }
        return result;
    }

    std::shared_ptr<NestedColumnType> typed_nested_data_;
};

template <typename T>
constexpr bool IsNullable = std::is_base_of_v<ColumnNullable, T>;

}
