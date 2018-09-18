#include "table_reader.h"

#include "private.h"

#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/concurrency/scheduler.h>

#include <util/generic/string.h>
#include <util/system/compiler.h>

#include <functional>
#include <vector>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
static void ProduceValue(NInterop::IColumnBuilder& column, T value)
{
    static_cast<NInterop::ITypedColumnBuilder<T> &>(column).Add(&value, 1);
}

static void ProduceStringValue(NInterop::IColumnBuilder& column, TStringBuf value)
{
    static_cast<NInterop::IStringColumnBuilder &>(column).Add(&value, 1);
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

    static void Produce(NInterop::IColumnBuilder& columnBuilder, const TInt value)
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

    static void Produce(NInterop::IColumnBuilder& columnBuilder, const TUInt value)
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

    static void Produce(NInterop::IColumnBuilder& columnBuilder, const ui8 value)
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

    static void Produce(NInterop::IColumnBuilder& columnBuilder, const TFloat value)
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

    static void Produce(NInterop::IColumnBuilder& columnBuilder, const TStringBuf value)
    {
        ProduceStringValue(columnBuilder, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

using THandlerFunction = std::function<
    void(NInterop::IColumnBuilder& columnBuilder, const TUnversionedValue& value)
>;

class TColumnHandlerFactory
{
private:
    template <class TTypedColumnTraits>
    static THandlerFunction CreateHandlerFunction(const NInterop::TColumn& columnSchema)
    {
        return [columnSchema](
            NInterop::IColumnBuilder& columnBuilder,
            const TUnversionedValue& value)
        {
            if (Y_LIKELY(value.Type != EValueType::Null)) {
                TTypedColumnTraits::Produce(
                    columnBuilder,
                    TTypedColumnTraits::Decode(value));
            } else {
                if (columnSchema.IsSorted()) {
                    THROW_ERROR_EXCEPTION("Null in key column")
                        << TErrorAttribute("column", columnSchema.Name);
                }
                TTypedColumnTraits::Produce(
                    columnBuilder,
                    TTypedColumnTraits::RepresentNullAsValue());
            }
        };
    }

public:
    static THandlerFunction Create(const NInterop::TColumn& schema)
    {
        switch (schema.Type) {
            /// Invalid type.
            case NInterop::EColumnType::Invalid:
                break;

            /// Signed integer value.
            case NInterop::EColumnType::Int8:
                return CreateHandlerFunction<TIntColumnTraits<i8>>(schema);
            case NInterop::EColumnType::Int16:
                return CreateHandlerFunction<TIntColumnTraits<i16>>(schema);
            case NInterop::EColumnType::Int32:
                return CreateHandlerFunction<TIntColumnTraits<i32>>(schema);
            case NInterop::EColumnType::Int64:
                return CreateHandlerFunction<TIntColumnTraits<i64>>(schema);

            /// Unsigned integer value.
            case NInterop::EColumnType::UInt8:
                return CreateHandlerFunction<TUIntColumnTraits<ui8>>(schema);
            case NInterop::EColumnType::UInt16:
                return CreateHandlerFunction<TUIntColumnTraits<ui16>>(schema);
            case NInterop::EColumnType::UInt32:
                return CreateHandlerFunction<TUIntColumnTraits<ui32>>(schema);
            case NInterop::EColumnType::UInt64:
                return CreateHandlerFunction<TUIntColumnTraits<ui64>>(schema);

            /// Floating point value.
            case NInterop::EColumnType::Float:
                return CreateHandlerFunction<TFloatColumnTraits<float>>(schema);
            case NInterop::EColumnType::Double:
                return CreateHandlerFunction<TFloatColumnTraits<double>>(schema);

            /// Boolean value.
            case NInterop::EColumnType::Boolean:
                return CreateHandlerFunction<TBooleanColumnTraits>(schema);

            /// DateTime value.
            case NInterop::EColumnType::Date:
                return CreateHandlerFunction<TUIntColumnTraits<ui16>>(schema);
            case NInterop::EColumnType::DateTime:
                return CreateHandlerFunction<TUIntColumnTraits<ui32>>(schema);

            /// String value.
            case NInterop::EColumnType::String:
                return CreateHandlerFunction<TStringColumnTraits>(schema);
        }

        THROW_ERROR_EXCEPTION("Invalid column type")
            << TErrorAttribute("column_type", static_cast<int>(schema.Type));
    }
};

////////////////////////////////////////////////////////////////////////////////

// System column handlers

THandlerFunction CreateSourceTableColumnHandler(
    const NInterop::TTableList& tables)
{
    auto handler = [tables] (NInterop::IColumnBuilder& columnBuilder, const TUnversionedValue& value)
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

NInterop::TColumnList ConcatColumns(
    NInterop::TColumnList physical,
    NInterop::TSystemColumns system)
{
    NInterop::TColumnList all;
    all.reserve(physical.size() + system.GetCount());

    all.insert(all.end(), physical.begin(), physical.end());

    const auto system_ = system.ToColumnList();
    all.insert(all.end(), system_.begin(), system_.end());

    return all;
}

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public NInterop::ITableReader
{
private:
    static const size_t MAX_ROWS_PER_READ = 1024;

    const NInterop::TTableList Tables;

    NInterop::TColumnList PhysicalColumns;
    NInterop::TSystemColumns SystemColumns;
    NInterop::TColumnList AllColumns;

    const ISchemafulReaderPtr ChunkReader;

    using TRows = std::vector<TUnversionedRow>;
    TRows Rows;

    std::vector<THandlerFunction> ColumnHandlers;

public:
    TTableReader(NInterop::TTableList tables,
                 NInterop::TColumnList columns,
                 NInterop::TSystemColumns systemColumns,
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

    const NInterop::TTableList& GetTables() const override
    {
        return Tables;
    }

    NInterop::TColumnList GetColumns() const override
    {
        return AllColumns;
    }

    bool Read(const NInterop::TColumnBuilderList& columns) override;

private:
    void PrepareColumnHandlers();

    bool TryReadBunchOfRows(TRows* rows);
    void ForwardRows(const TRows& rows, const NInterop::TColumnBuilderList& columns);
    void ValidateColumns(const NInterop::TColumnBuilderList& columns) const;
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

bool TTableReader::Read(const NInterop::TColumnBuilderList& columns)
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
    const NInterop::TColumnBuilderList& columns)
{
    for (const auto& row: rows) {
        // row schema already validated by SchemafulReader
        YCHECK(row.GetCount() == columns.size());

        for (size_t i = 0; i < row.GetCount(); ++i) {
            ColumnHandlers[i](*columns[i], row[i]);
        }
    }
}

void TTableReader::ValidateColumns(const NInterop::TColumnBuilderList& columns) const
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

NInterop::ITableReaderPtr CreateTableReader(
    NInterop::TTableList tables,
    NInterop::TColumnList columns,
    NInterop::TSystemColumns systemColumns,
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

NInterop::ITableReaderPtr CreateTableReader(
    NInterop::TTablePtr table,
    ISchemafulReaderPtr chunkReader)
{
    return CreateTableReader(
        {table},
        table->Columns,
        {},
        std::move(chunkReader));
}

}   // namespace NClickHouse
}   // namespace NYT
