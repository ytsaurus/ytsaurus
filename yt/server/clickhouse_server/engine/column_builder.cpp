#include "column_builder.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

namespace DB {

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TTypedColumnBuilder
    : public NNative::ITypedColumnBuilder<T>
{
private:
    const MutableColumnPtr Column;

public:
    TTypedColumnBuilder(NNative::EColumnType type, MutableColumnPtr column)
        : Column(std::move(column))
    {
        NNative::IColumnBuilder::Type = type;
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
    : public NNative::IStringColumnBuilder
{
private:
    MutableColumnPtr Column;

public:
    TStringColumnBuilder(NNative::EColumnType type, MutableColumnPtr column)
        : Column(std::move(column))
    {
        NNative::IColumnBuilder::Type = type;
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

NNative::IColumnBuilderPtr CreateColumnBuilder(
    NNative::EColumnType type,
    MutableColumnPtr column)
{
    switch (type) {
        /// Invalid type.
        case NNative::EColumnType::Invalid:
            break;

        /// Signed integer value.
        case NNative::EColumnType::Int8:
            return std::make_shared<TTypedColumnBuilder<Int8>>(type, std::move(column));
        case NNative::EColumnType::Int16:
            return std::make_shared<TTypedColumnBuilder<Int16>>(type, std::move(column));
        case NNative::EColumnType::Int32:
            return std::make_shared<TTypedColumnBuilder<Int32>>(type, std::move(column));
        case NNative::EColumnType::Int64:
            return std::make_shared<TTypedColumnBuilder<Int64>>(type, std::move(column));

        /// Unsigned integer value.
        case NNative::EColumnType::UInt8:
            return std::make_shared<TTypedColumnBuilder<UInt8>>(type, std::move(column));
        case NNative::EColumnType::UInt16:
            return std::make_shared<TTypedColumnBuilder<UInt16>>(type, std::move(column));
        case NNative::EColumnType::UInt32:
            return std::make_shared<TTypedColumnBuilder<UInt32>>(type, std::move(column));
        case NNative::EColumnType::UInt64:
            return std::make_shared<TTypedColumnBuilder<UInt64>>(type, std::move(column));

        /// Floating point value.
        case NNative::EColumnType::Float:
            return std::make_shared<TTypedColumnBuilder<Float32>>(type, std::move(column));
        case NNative::EColumnType::Double:
            return std::make_shared<TTypedColumnBuilder<Float64>>(type, std::move(column));

        /// Boolean value.
        case NNative::EColumnType::Boolean:
            return std::make_shared<TTypedColumnBuilder<UInt8>>(type, std::move(column));

        /// DateTime value.
        case NNative::EColumnType::Date:
            return std::make_shared<TTypedColumnBuilder<UInt16>>(type, std::move(column));
        case NNative::EColumnType::DateTime:
            return std::make_shared<TTypedColumnBuilder<UInt32>>(type, std::move(column));

        /// String value.
        case NNative::EColumnType::String:
            return std::make_shared<TStringColumnBuilder>(type, std::move(column));
    }

    throw Exception(
        "Invalid column data type",
        toString(static_cast<int>(type)),
        ErrorCodes::UNKNOWN_TYPE);
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
