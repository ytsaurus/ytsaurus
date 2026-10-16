#pragma once

#include "column.h"
#include "numeric.h"
#include "utils.h"

#include <memory>

namespace clickhouse {

template <typename NestedColumnType>
class ColumnArrayT;

/**
 * Represents column of Array(T).
 */
class ColumnArray : public Column {
public:
    using ValueType = ColumnRef;

    /** Create an array of given type.
     *
     *  `data` is used internally (and modified) by ColumnArray.
     *  Users are strongly advised against supplying non-empty columns and/or modifying
     *  contents of `data` afterwards.
     */
    explicit ColumnArray(ColumnRef data);

    /** Create an array of given type, with actual values and offsets.
     *
     *  Both `data` and `offsets` are used (and modified) internally bye ColumnArray.
     *  Users are strongly advised against modifying contents of `data` or `offsets` afterwards.
     */
    ColumnArray(ColumnRef data, std::shared_ptr<ColumnUInt64> offsets);

    /// Converts input column to array and appends as one row to the current column.
    void AppendAsColumn(ColumnRef array);

    /// Converts array at pos n to column.
    /// Type of element of result column same as type of array element.
    ColumnRef GetAsColumn(size_t n) const;

    /// Shorthand to get a column casted to a proper type.
    template <typename T>
    auto GetAsColumnTyped(size_t n) const {
        return GetAsColumn(n)->AsStrict<T>();
    }

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
    ColumnRef Slice(size_t, size_t) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column&) override;

    void OffsetsIncrease(size_t);

    /// Gets the backing data array of the Array's. This does not include any Array Bounds.
    ColumnRef GetData();
    std::shared_ptr<const Column> GetData() const;

    /// Gets all offsets denoting the list boundaries overlayed GetData.
    /// The layout is [size_i, ...] where `i` is the row.
    std::shared_ptr<ColumnUInt64>& GetOffsets();
    std::shared_ptr<const ColumnUInt64> GetOffsets() const;

    /// Gets the offset of the start of row `n` into `GetData()`.
    size_t GetOffset(size_t n) const;

    /// Gets the element count of row `n`.
    size_t GetSize(size_t n) const;

protected:
    template<typename T> friend class ColumnArrayT;

    ColumnArray(ColumnArray&& array);

    void AddOffset(size_t n);
    void Reset();

private:
    ColumnRef data_;
    std::shared_ptr<ColumnUInt64> offsets_;
};

template <typename ColumnType>
class ColumnArrayT : public ColumnArray {
public:
    class ArrayValueView;
    using ValueType = ArrayValueView;
    using NestedColumnType = ColumnType;

    explicit ColumnArrayT(std::shared_ptr<NestedColumnType> data)
        : ColumnArray(data)
        , typed_nested_data_(data)
    {}

    ColumnArrayT(std::shared_ptr<NestedColumnType> data, std::shared_ptr<ColumnUInt64> offsets)
        : ColumnArray(data, offsets)
        , typed_nested_data_(data)
    {}

    template <typename ...Args>
    explicit ColumnArrayT(Args &&... args)
        : ColumnArrayT(std::make_shared<NestedColumnType>(std::forward<Args>(args)...))
    {}

    /** Create a ColumnArrayT from a ColumnArray, without copying data and offsets, but by 'stealing' those from `col`.
     *
     *  Ownership of column internals is transferred to returned object, original (argument) object
     *  MUST NOT BE USED IN ANY WAY, it is only safe to dispose it.
     *
     *  Throws an exception if `col` is of wrong type, it is safe to use original col in this case.
     *  This is a static method to make such conversion verbose.
     */
    static auto Wrap(ColumnArray&& col) {
        auto nested_data = WrapColumn<NestedColumnType>(col.GetData());
        return std::make_shared<ColumnArrayT<NestedColumnType>>(nested_data, col.offsets_);
    }

    static auto Wrap(Column&& col) {
        return Wrap(std::move(dynamic_cast<ColumnArray&&>(col)));
    }

    // Helper to simplify integration with other APIs
    static auto Wrap(ColumnRef&& col) {
        return Wrap(std::move(*col->AsStrict<ColumnArray>()));
    }

    /// A single (row) value of the Array-column, i.e. readonly array of items.
    class ArrayValueView {
        const std::shared_ptr<NestedColumnType> typed_nested_data_;
        const size_t offset_;
        const size_t size_;

    public:
        using ValueType = std::decay_t<decltype(std::declval<NestedColumnType>().At(0))>;

        ArrayValueView(std::shared_ptr<NestedColumnType> data, size_t offset = 0, size_t size = std::numeric_limits<size_t>::max())
            : typed_nested_data_(data)
            , offset_(offset)
            , size_(std::min(typed_nested_data_->Size() - offset, size))
        {}

        inline auto operator[](size_t index) const {
            return (*typed_nested_data_)[offset_ + index];
        }

        inline auto At(size_t index) const {
            if (index >= size_)
                throw ValidationError("ColumnArray value index out of bounds: "
                        + std::to_string(index) + ", max is " + std::to_string(size_));
            return typed_nested_data_->At(offset_ + index);
        }

        class Iterator {
            const std::shared_ptr<NestedColumnType> typed_nested_data_;
            const size_t offset_;
            const size_t size_;
            size_t index_;
        public:
            Iterator() = default;

            Iterator(std::shared_ptr<NestedColumnType> typed_nested_data, size_t offset, size_t size, size_t index)
                : typed_nested_data_(typed_nested_data)
                , offset_(offset)
                , size_(size)
                , index_(index)
            {}

            using ValueType = typename ArrayValueView::ValueType;

            inline auto operator*() const {
                return typed_nested_data_->At(offset_ + index_);
            }

            inline Iterator& operator++() {
                ++index_;
                return *this;
            }

            inline bool operator==(const Iterator& other) const {
                return this->typed_nested_data_ == other.typed_nested_data_
                        && this->offset_ == other.offset_
                        && this->size_ == other.size_
                        && this->index_ == other.index_;
            }

            inline bool operator!=(const Iterator& other) const {
                return !(*this == other);
            }
        };

        // minimalistic stl-like container interface, hence the lowercase
        inline Iterator begin() const {
            return Iterator{typed_nested_data_, offset_, size_, 0};
        }

        inline Iterator cbegin() const {
            return Iterator{typed_nested_data_, offset_, size_, 0};
        }

        inline Iterator end() const {
            return Iterator{typed_nested_data_, offset_, size_, size_};
        }

        inline Iterator cend() const {
            return Iterator{typed_nested_data_, offset_, size_, size_};
        }

        inline size_t size() const {
            return size_;
        }

        // It is ugly to have both size() and Size(), but it is for compatitability with both STL and rest of the clickhouse-cpp.
        inline size_t Size() const {
            return size_;
        }

        inline bool operator==(const ArrayValueView& other) const {
            if (size() != other.size()) {
                return false;
            }
            for (size_t i = 0; i < size_; ++i) {
                if ((*this)[i] != other[i]) {
                    return false;
                }
            }
            return true;
        }

        inline bool operator!=(const ArrayValueView& other) const {
            return !(*this == other);
        }
    };

    inline auto At(size_t index) const {
        if (index >= Size())
            throw ValidationError("ColumnArray row index out of bounds: "
                    + std::to_string(index) + ", max is " + std::to_string(Size()));

        return ArrayValueView{typed_nested_data_, GetOffset(index), GetSize(index)};
    }

    inline auto operator[](size_t index) const {
        return ArrayValueView{typed_nested_data_, GetOffset(index), GetSize(index)};
    }

    using ColumnArray::Append;

    template <typename Container>
    inline void Append(Container&& container) {
        using container_type = decltype(container);
        if constexpr (std::is_lvalue_reference_v<container_type> || std::is_const_v<std::remove_reference_t<container_type>>) {
            Append(std::begin(container), std::end(container));
        } else {
            Append(std::make_move_iterator(std::begin(container)),
                std::make_move_iterator(std::end(container)));
        }
    }

    template <typename ValueType>
    inline void Append(const std::initializer_list<ValueType>& container) {
        Append(std::begin(container), std::end(container));
    }

    template <typename Begin, typename End>
    inline void Append(Begin begin, End end) {
        auto & nested_data = *typed_nested_data_;
        size_t counter = 0;

        while (begin != end) {
            nested_data.Append(*begin);
            ++begin;
            ++counter;
        }

        // Even if there are 0 items, increase counter, creating empty array item.
        AddOffset(counter);
    }

    ColumnRef Slice(size_t begin, size_t size) const override {
        return Wrap(ColumnArray::Slice(begin, size));
    }

    ColumnRef CloneEmpty() const override {
        return Wrap(ColumnArray::CloneEmpty());
    }

    void Swap(Column& other) override {
        auto & col = dynamic_cast<ColumnArrayT<NestedColumnType> &>(other);
        typed_nested_data_.swap(col.typed_nested_data_);
        ColumnArray::Swap(other);
    }

private:
    /// Helper to allow wrapping a "typeless" ColumnArray
    ColumnArrayT(ColumnArray&& array, std::shared_ptr<NestedColumnType> nested_data)
        : ColumnArray(std::move(array))
        , typed_nested_data_(std::move(nested_data))
    {}


private:
    std::shared_ptr<NestedColumnType> typed_nested_data_;
};

}
