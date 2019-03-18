#include "column_builder.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TTypedColumnBuilder
    : public ITypedColumnBuilder<T>
{
private:
    const MutableColumnPtr Column;

public:
    TTypedColumnBuilder(EClickHouseColumnType type, MutableColumnPtr column)
        : Column(std::move(column))
    {
        IColumnBuilder::Type = type;
    }

    void Add(const T* values, size_t count) override
    {
        auto& typedColumn = typeid_cast<ColumnVector<T> &>(*Column);
        auto& data = typedColumn.getData();

        size_t oldSize = data.size();
        data.resize(oldSize + count);

        memcpy(&data[oldSize], values, count * sizeof(T));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStringColumnBuilder
    : public IStringColumnBuilder
{
private:
    MutableColumnPtr Column;

public:
    TStringColumnBuilder(EClickHouseColumnType type, MutableColumnPtr column)
        : Column(std::move(column))
    {
        IColumnBuilder::Type = type;
    }

    void Add(const TStringBuf* values, size_t count) override
    {
        auto& typedColumn = typeid_cast<ColumnString &>(*Column);
        auto& data = typedColumn.getChars();
        auto& offsets = typedColumn.getOffsets();

        size_t totalSize = 0;
        for (size_t i = 0; i < count; ++i) {
            totalSize += values[i].size() + 1; // reserve slot for terminating zero byte
        }

        ui64 offset = data.size();
        data.resize(offset + totalSize);

        size_t oldSize = offsets.size();
        offsets.resize(oldSize + count);

        for (size_t i = 0; i < count; ++i) {
            const size_t valueLength = values[i].size();
            memcpy(&data[offset], values[i].data(), valueLength);
            data[offset + valueLength] = 0;

            offset += valueLength + 1;
            offsets[oldSize + i] = offset;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IColumnBuilderPtr CreateColumnBuilder(
    EClickHouseColumnType type,
    MutableColumnPtr column)
{
    switch (type) {
        /// Invalid type.
        case EClickHouseColumnType::Invalid:
            break;

        /// Signed integer value.
        case EClickHouseColumnType::Int8:
            return std::make_shared<TTypedColumnBuilder<Int8>>(type, std::move(column));
        case EClickHouseColumnType::Int16:
            return std::make_shared<TTypedColumnBuilder<Int16>>(type, std::move(column));
        case EClickHouseColumnType::Int32:
            return std::make_shared<TTypedColumnBuilder<Int32>>(type, std::move(column));
        case EClickHouseColumnType::Int64:
            return std::make_shared<TTypedColumnBuilder<Int64>>(type, std::move(column));

        /// Unsigned integer value.
        case EClickHouseColumnType::UInt8:
            return std::make_shared<TTypedColumnBuilder<UInt8>>(type, std::move(column));
        case EClickHouseColumnType::UInt16:
            return std::make_shared<TTypedColumnBuilder<UInt16>>(type, std::move(column));
        case EClickHouseColumnType::UInt32:
            return std::make_shared<TTypedColumnBuilder<UInt32>>(type, std::move(column));
        case EClickHouseColumnType::UInt64:
            return std::make_shared<TTypedColumnBuilder<UInt64>>(type, std::move(column));

        /// Floating point value.
        case EClickHouseColumnType::Float:
            return std::make_shared<TTypedColumnBuilder<Float32>>(type, std::move(column));
        case EClickHouseColumnType::Double:
            return std::make_shared<TTypedColumnBuilder<Float64>>(type, std::move(column));

        /// Boolean value.
        case EClickHouseColumnType::Boolean:
            return std::make_shared<TTypedColumnBuilder<UInt8>>(type, std::move(column));

        /// DateTime value.
        case EClickHouseColumnType::Date:
            return std::make_shared<TTypedColumnBuilder<UInt16>>(type, std::move(column));
        case EClickHouseColumnType::DateTime:
            return std::make_shared<TTypedColumnBuilder<UInt32>>(type, std::move(column));

        /// String value.
        case EClickHouseColumnType::String:
            return std::make_shared<TStringColumnBuilder>(type, std::move(column));
    }

    throw Exception(
        "Invalid column data type",
        Exception(toString(static_cast<int>(type)), ErrorCodes::UNKNOWN_TYPE),
        ErrorCodes::UNKNOWN_TYPE);
}

} // namespace NYT::NClickHouseServer
