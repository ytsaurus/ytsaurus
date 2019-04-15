#include "table_reader.h"

#include "private.h"
#include "helpers.h"
#include "column_builder.h"

#include <yt/ytlib/api/native/table_reader.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <yt/client/api/client.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/table_client/schemaful_reader_adapter.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/concurrency/scheduler.h>

#include <util/generic/string.h>
#include <util/system/compiler.h>

#include <functional>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYPath;
using namespace NLogging;
using namespace NYTree;

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
    static THandlerFunction CreateHandlerFunction(const TClickHouseColumn& columnSchema)
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
    static THandlerFunction Create(const TClickHouseColumn& schema)
    {
        switch (schema.Type) {
            /// Invalid type.
            case EClickHouseColumnType::Invalid:
                break;

            /// Signed integer value.
            case EClickHouseColumnType::Int8:
                return CreateHandlerFunction<TIntColumnTraits<i8>>(schema);
            case EClickHouseColumnType::Int16:
                return CreateHandlerFunction<TIntColumnTraits<i16>>(schema);
            case EClickHouseColumnType::Int32:
                return CreateHandlerFunction<TIntColumnTraits<i32>>(schema);
            case EClickHouseColumnType::Int64:
                return CreateHandlerFunction<TIntColumnTraits<i64>>(schema);

            /// Unsigned integer value.
            case EClickHouseColumnType::UInt8:
                return CreateHandlerFunction<TUIntColumnTraits<ui8>>(schema);
            case EClickHouseColumnType::UInt16:
                return CreateHandlerFunction<TUIntColumnTraits<ui16>>(schema);
            case EClickHouseColumnType::UInt32:
                return CreateHandlerFunction<TUIntColumnTraits<ui32>>(schema);
            case EClickHouseColumnType::UInt64:
                return CreateHandlerFunction<TUIntColumnTraits<ui64>>(schema);

            /// Floating point value.
            case EClickHouseColumnType::Float:
                return CreateHandlerFunction<TFloatColumnTraits<float>>(schema);
            case EClickHouseColumnType::Double:
                return CreateHandlerFunction<TFloatColumnTraits<double>>(schema);

            /// Boolean value.
            case EClickHouseColumnType::Boolean:
                return CreateHandlerFunction<TBooleanColumnTraits>(schema);

            /// DateTime value.
            case EClickHouseColumnType::Date:
                return CreateHandlerFunction<TUIntColumnTraits<ui16>>(schema);
            case EClickHouseColumnType::DateTime:
                return CreateHandlerFunction<TUIntColumnTraits<ui32>>(schema);

            /// String value.
            case EClickHouseColumnType::String:
                return CreateHandlerFunction<TStringColumnTraits>(schema);
        }

        THROW_ERROR_EXCEPTION("Invalid column type")
            << TErrorAttribute("column_type", static_cast<int>(schema.Type));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public ITableReader
{
private:
    static const size_t MAX_ROWS_PER_READ = 1024;

    std::vector<TClickHouseColumn> Columns;

    const ISchemafulReaderPtr ChunkReader;

    using TRows = std::vector<TUnversionedRow>;
    TRows Rows;

    std::vector<THandlerFunction> ColumnHandlers;

public:
    TTableReader(std::vector<TClickHouseColumn> columns,
                 ISchemafulReaderPtr chunkReader)
        : Columns(std::move(columns))
        , ChunkReader(std::move(chunkReader))
    {
        Rows.reserve(MAX_ROWS_PER_READ);

        PrepareColumnHandlers();
    }

    std::vector<TClickHouseColumn> GetColumns() const override
    {
        return Columns;
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
    ColumnHandlers.reserve(Columns.size());
    for (const auto& columnSchema: Columns) {
        ColumnHandlers.push_back(TColumnHandlerFactory::Create(columnSchema));
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
    if (columns.size() != Columns.size()) {
        THROW_ERROR_EXCEPTION("Mismatched columns count")
            << TErrorAttribute("expected", Columns.size())
            << TErrorAttribute("actual", columns.size());
    }

    for (size_t i = 0; i < Columns.size(); ++i) {
        if (static_cast<int>(columns[i]->GetType()) != static_cast<int>(Columns[i].Type)) {
            THROW_ERROR_EXCEPTION("Mismatched column type")
                << TErrorAttribute("column", Columns[i].Name)
                << TErrorAttribute("expected", static_cast<int>(Columns[i].Type))
                << TErrorAttribute("actual", static_cast<int>(columns[i]->GetType()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ITableReaderPtr CreateTableReader(
    std::vector<TClickHouseColumn> columns,
    ISchemafulReaderPtr chunkReader)
{
    return std::make_shared<TTableReader>(
        std::move(columns),
        std::move(chunkReader));
}

/////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulTableReader(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath& path,
    const TTableSchema& schema,
    const NApi::TTableReaderOptions& options,
    const TColumnFilter& columnFilter)
{
    auto schemalessReaderFactory = [=] (
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) -> ISchemalessReaderPtr
    {
        auto result = WaitFor(
            NApi::NNative::CreateSchemalessMultiChunkReader(
                client,
                path,
                options,
                nameTable,
                columnFilter))
            .ValueOrThrow();
        return result.Reader;
    };

    return CreateSchemafulReaderAdapter(
        std::move(schemalessReaderFactory),
        schema,
        columnFilter);
}

/////////////////////////////////////////////////////////////////////////////

ITableReaderPtr CreateTableReader(const NApi::NNative::IClientPtr& client, const TRichYPath& path, bool unordered, TLogger logger)
{
    const auto& Logger = logger;

    YT_LOG_INFO("Creating table reader (Path: %v)", path);

    auto tableObject = GetTableAttributes(
        client,
        path,
        EPermission::Read,
        logger);

    NApi::TTableReaderOptions readerOptions;
    readerOptions.Unordered = unordered;
    readerOptions.Config = New<NTableClient::TTableReaderConfig>();
    // TODO(max42): YT-10134.
    readerOptions.Config->FetchFromPeers = false;

    auto chunkReader = CreateSchemafulTableReader(
        client,
        path,
        tableObject->Schema,
        readerOptions);

    // TODO(max42): rename?
    auto readerTable = std::make_shared<TClickHouseTable>(path.GetPath(), tableObject->Schema);
    return NClickHouseServer::CreateTableReader(readerTable->Columns, std::move(chunkReader));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
