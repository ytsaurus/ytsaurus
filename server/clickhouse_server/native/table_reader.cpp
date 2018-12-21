#include "table_reader.h"

#include "private.h"

#include "column_builder.h"
#include "system_columns.h"

#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/concurrency/scheduler.h>

#include <util/generic/string.h>
#include <util/system/compiler.h>

#include <functional>
#include <vector>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
static void ProduceValue(IColumnBuilder& column, T value)
{
    static_cast<ITypedColumnBuilder<T> &>(column).Add(&value, 1);
}

static void ProduceStringValue(IColumnBuilder& column, TStringBuf value)
{
    static_cast<IStringColumnBuilder &>(column).Add(&value, 1);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TInt>
struct TIntColumnTraits
{
    static TInt Decode(const TUnversionedValue& value)
    {
        return static_cast<TInt>(value.Data.Int64);
    }

    static TInt RepresentNullAsValue()
    {
        return 0;
    }

    static void Produce(IColumnBuilder& columnBuilder, const TInt value)
    {
        ProduceValue(columnBuilder, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TUInt>
struct TUIntColumnTraits
{
    static TUInt Decode(const TUnversionedValue& value)
    {
        return static_cast<TUInt>(value.Data.Uint64);
    }

    static TUInt RepresentNullAsValue()
    {
        return 0u;
    }

    static void Produce(IColumnBuilder& columnBuilder, const TUInt value)
    {
        ProduceValue(columnBuilder, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBooleanColumnTraits
{
    static ui8 Decode(const TUnversionedValue& value)
    {
        return value.Data.Boolean ? 1 : 0;
    }

    static ui8 RepresentNullAsValue()
    {
        return 0u;
    }

    static void Produce(IColumnBuilder& columnBuilder, const ui8 value)
    {
        ProduceValue(columnBuilder, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TFloat>
struct TFloatColumnTraits
{
    static TFloat Decode(const TUnversionedValue& value)
    {
        return static_cast<TFloat>(value.Data.Double);
    }

    static TFloat RepresentNullAsValue()
    {
        return 0.0;
    }

    static void Produce(IColumnBuilder& columnBuilder, const TFloat value)
    {
        ProduceValue(columnBuilder, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStringColumnTraits
{
    static TStringBuf Decode(const TUnversionedValue& value)
    {
        return {value.Data.String, value.Length};
    }

    static TStringBuf RepresentNullAsValue()
    {
        return {};
    }

    static void Produce(IColumnBuilder& columnBuilder, const TStringBuf value)
    {
        ProduceStringValue(columnBuilder, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

using THandlerFunction = std::function<
    void(IColumnBuilder& columnBuilder, const TUnversionedValue& value)
>;

class TColumnHandlerFactory
{
private:
    template <class TTypedColumnTraits>
    static THandlerFunction CreateHandlerFunction(const TColumn& columnSchema)
    {
        return [columnSchema](
            IColumnBuilder& columnBuilder,
            const TUnversionedValue& value)
        {
            if (Y_LIKELY(value.Type != EValueType::Null)) {
                TTypedColumnTraits::Produce(
                    columnBuilder,
                    TTypedColumnTraits::Decode(value));
            } else {
                if (columnSchema.IsSorted()) {
                    THROW_ERROR_EXCEPTION("std::nullopt in key column")
                        << TErrorAttribute("column", columnSchema.Name);
                }
                TTypedColumnTraits::Produce(
                    columnBuilder,
                    TTypedColumnTraits::RepresentNullAsValue());
            }
        };
    }

public:
    static THandlerFunction Create(const TColumn& schema)
    {
        switch (schema.Type) {
            /// Invalid type.
            case EColumnType::Invalid:
                break;

            /// Signed integer value.
            case EColumnType::Int8:
                return CreateHandlerFunction<TIntColumnTraits<i8>>(schema);
            case EColumnType::Int16:
                return CreateHandlerFunction<TIntColumnTraits<i16>>(schema);
            case EColumnType::Int32:
                return CreateHandlerFunction<TIntColumnTraits<i32>>(schema);
            case EColumnType::Int64:
                return CreateHandlerFunction<TIntColumnTraits<i64>>(schema);

            /// Unsigned integer value.
            case EColumnType::UInt8:
                return CreateHandlerFunction<TUIntColumnTraits<ui8>>(schema);
            case EColumnType::UInt16:
                return CreateHandlerFunction<TUIntColumnTraits<ui16>>(schema);
            case EColumnType::UInt32:
                return CreateHandlerFunction<TUIntColumnTraits<ui32>>(schema);
            case EColumnType::UInt64:
                return CreateHandlerFunction<TUIntColumnTraits<ui64>>(schema);

            /// Floating point value.
            case EColumnType::Float:
                return CreateHandlerFunction<TFloatColumnTraits<float>>(schema);
            case EColumnType::Double:
                return CreateHandlerFunction<TFloatColumnTraits<double>>(schema);

            /// Boolean value.
            case EColumnType::Boolean:
                return CreateHandlerFunction<TBooleanColumnTraits>(schema);

            /// DateTime value.
            case EColumnType::Date:
                return CreateHandlerFunction<TUIntColumnTraits<ui16>>(schema);
            case EColumnType::DateTime:
                return CreateHandlerFunction<TUIntColumnTraits<ui32>>(schema);

            /// String value.
            case EColumnType::String:
                return CreateHandlerFunction<TStringColumnTraits>(schema);
        }

        THROW_ERROR_EXCEPTION("Invalid column type")
            << TErrorAttribute("column_type", static_cast<int>(schema.Type));
    }
};

////////////////////////////////////////////////////////////////////////////////

// System column handlers

THandlerFunction CreateSourceTableColumnHandler(
    const TTableList& tables)
{
    auto handler = [tables] (IColumnBuilder& columnBuilder, const TUnversionedValue& value)
    {
        const i64 tableIndex = value.Data.Int64;

        if (tableIndex < 0 || tableIndex >= static_cast<int>(tables.size())) {
            THROW_ERROR_EXCEPTION("table index is out of range")
                << TErrorAttribute("table_index", tableIndex);
        }

        ProduceStringValue(columnBuilder, tables[tableIndex]->Name);
    };

    return handler;
}

////////////////////////////////////////////////////////////////////////////////

TColumnList ConcatColumns(
    TColumnList physical,
    TSystemColumns system)
{
    TColumnList all;
    all.reserve(physical.size() + system.GetCount());

    all.insert(all.end(), physical.begin(), physical.end());

    const auto system_ = system.ToColumnList();
    all.insert(all.end(), system_.begin(), system_.end());

    return all;
}

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public ITableReader
{
private:
    static const size_t MAX_ROWS_PER_READ = 1024;

    const TTableList Tables;

    TColumnList PhysicalColumns;
    TSystemColumns SystemColumns;
    TColumnList AllColumns;

    const ISchemafulReaderPtr ChunkReader;

    using TRows = std::vector<TUnversionedRow>;
    TRows Rows;

    std::vector<THandlerFunction> ColumnHandlers;

public:
    TTableReader(TTableList tables,
                 TColumnList columns,
                 TSystemColumns systemColumns,
                 ISchemafulReaderPtr chunkReader)
        : Tables(std::move(tables))
        , PhysicalColumns(std::move(columns))
        , SystemColumns(std::move(systemColumns))
        , AllColumns(ConcatColumns(PhysicalColumns, SystemColumns))
        , ChunkReader(std::move(chunkReader))
    {
        Rows.reserve(MAX_ROWS_PER_READ);

        PrepareColumnHandlers();
    }

    const TTableList& GetTables() const override
    {
        return Tables;
    }

    TColumnList GetColumns() const override
    {
        return AllColumns;
    }

    bool Read(const TColumnBuilderList& columns) override;

private:
    void PrepareColumnHandlers();

    bool TryReadBunchOfRows(TRows* rows);
    void ForwardRows(const TRows& rows, const TColumnBuilderList& columns);
    void ValidateColumns(const TColumnBuilderList& columns) const;
};

////////////////////////////////////////////////////////////////////////////////

void TTableReader::PrepareColumnHandlers()
{
    ColumnHandlers.reserve(PhysicalColumns.size());
    for (const auto& columnSchema: PhysicalColumns) {
        ColumnHandlers.push_back(TColumnHandlerFactory::Create(columnSchema));
    }

    if (SystemColumns.TableName.Defined()) {
        ColumnHandlers.push_back(
            CreateSourceTableColumnHandler(Tables));
    }
}

bool TTableReader::Read(const TColumnBuilderList& columns)
{
    ValidateColumns(columns);

    if (!TryReadBunchOfRows(&Rows)) {
        return false;
    }

    ForwardRows(Rows, columns);
    return true;
}

bool TTableReader::TryReadBunchOfRows(TRows* rows)
{
    rows->clear();
    for (;;) {
        if (!ChunkReader->Read(rows)) {
            return false;
        }
        if (!Rows.empty()) {
            return true;
        }
        WaitFor(ChunkReader->GetReadyEvent())
            .ThrowOnError();
    }

    Y_UNREACHABLE();
}

void TTableReader::ForwardRows(
    const TRows& rows,
    const TColumnBuilderList& columns)
{
    for (const auto& row: rows) {
        // row schema already validated by SchemafulReader
        YCHECK(row.GetCount() == columns.size());

        for (size_t i = 0; i < row.GetCount(); ++i) {
            ColumnHandlers[i](*columns[i], row[i]);
        }
    }
}

void TTableReader::ValidateColumns(const TColumnBuilderList& columns) const
{
    if (columns.size() != AllColumns.size()) {
        THROW_ERROR_EXCEPTION("Mismatched columns count")
            << TErrorAttribute("expected", AllColumns.size())
            << TErrorAttribute("actual", columns.size());
    }

    for (size_t i = 0; i < AllColumns.size(); ++i) {
        if (static_cast<int>(columns[i]->GetType()) != static_cast<int>(AllColumns[i].Type)) {
            THROW_ERROR_EXCEPTION("Mismatched column type")
                << TErrorAttribute("column", AllColumns[i].Name)
                << TErrorAttribute("expected", static_cast<int>(AllColumns[i].Type))
                << TErrorAttribute("actual", static_cast<int>(columns[i]->GetType()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ITableReaderPtr CreateTableReader(
    TTableList tables,
    TColumnList columns,
    TSystemColumns systemColumns,
    ISchemafulReaderPtr chunkReader)
{
    if (tables.empty()) {
        THROW_ERROR_EXCEPTION("Cannot create reader: provided empty list of tables");
    }

    return std::make_shared<TTableReader>(
        std::move(tables),
        std::move(columns),
        std::move(systemColumns),
        std::move(chunkReader));
}

ITableReaderPtr CreateTableReader(
    TTablePtr table,
    ISchemafulReaderPtr chunkReader)
{
    return CreateTableReader(
        {table},
        table->Columns,
        {},
        std::move(chunkReader));
}

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
