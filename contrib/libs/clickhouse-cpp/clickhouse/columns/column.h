#pragma once

#include "../types/types.h"
#include "../columns/itemview.h"
#include "../exceptions.h"

#include <memory>
#include <stdexcept>

namespace clickhouse {

class InputStream;
class OutputStream;

using ColumnRef = std::shared_ptr<class Column>;

/**
 * An abstract base of all columns classes.
 */
class Column : public std::enable_shared_from_this<Column> {
public:
    explicit inline Column(TypeRef type) : type_(type) {}

    virtual ~Column() {}

    /// Downcast pointer to the specific column's subtype.
    template <typename T>
    inline std::shared_ptr<T> As() {
        return std::dynamic_pointer_cast<T>(shared_from_this());
    }

    /// Downcast pointer to the specific column's subtype.
    template <typename T>
    inline std::shared_ptr<const T> As() const {
        return std::dynamic_pointer_cast<const T>(shared_from_this());
    }

    /// Downcast pointer to the specific column's subtype.
    template <typename T>
    inline std::shared_ptr<T> AsStrict() {
        auto result = std::dynamic_pointer_cast<T>(shared_from_this());
        if (!result) {
            throw ValidationError("Can't cast from " + type_->GetName());
        }
        return result;
    }

    /// Get type object of the column.
    inline TypeRef Type() const { return type_; }
    inline const class Type& GetType() const { return *type_; }

    /// Appends content of given column to the end of current one.
    virtual void Append(ColumnRef column) = 0;

    /// Increase the capacity of the column for large block insertion.
    virtual void Reserve(size_t new_cap) = 0;

    /// Template method to load column data from input stream. It'll call LoadPrefix and LoadBody.
    /// Should be called only once from the client. Derived classes should not call it.
    bool Load(InputStream* input, size_t rows);

    /// Loads column prefix from input stream.
    virtual bool LoadPrefix(InputStream* input, size_t rows);

    /// Loads column data from input stream.
    virtual bool LoadBody(InputStream* input, size_t rows) = 0;

    /// Saves column prefix to output stream. Column types with prefixes must implement it.
    virtual void SavePrefix(OutputStream* output);

    /// Saves column body to output stream.
    virtual void SaveBody(OutputStream* output) = 0;

    /// Template method to save to output stream. It'll call SavePrefix and SaveBody respectively
    /// Should be called only once from the client. Derived classes should not call it.
    /// Save is split in Prefix and Body because some data types require prefixes and specific serialization order.
    /// For instance, Array(LowCardinality(X)) requires LowCardinality.key_version bytes to come before Array.offsets
    void Save(OutputStream* output);

    /// Clear column data .
    virtual void Clear() = 0;

    /// Returns count of rows in the column.
    virtual size_t Size() const = 0;

    /// Makes slice of the current column.
    virtual ColumnRef Slice(size_t begin, size_t len) const = 0;

    virtual ColumnRef CloneEmpty() const = 0;

    virtual void Swap(Column&) = 0;

    /// Get a view on raw item data if it is supported by column, will throw an exception if index is out of range.
    /// Please note that view is invalidated once column items are added or deleted, column is loaded from strean or destroyed.
    virtual ItemView GetItem(size_t) const {
        throw UnimplementedError("GetItem() is not supported for column of " + type_->GetName());
    }

    friend void swap(Column& left, Column& right) {
        left.Swap(right);
    }

protected:
    TypeRef type_;
};

}  // namespace clickhouse
