#include "schemaful_reader_adapter.h"
#include "schemaless_row_reorderer.h"

#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/schemaless_reader.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/blob_output.h>

#include <yt/core/yson/writer.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemafulReaderAdapter)

struct TSchemafulReaderAdapterPoolTag { };

class TSchemafulReaderAdapter
    : public ISchemafulReader
{
public:
    TSchemafulReaderAdapter(
        ISchemalessReaderPtr underlyingReader,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns)
        : UnderlyingReader_(std::move(underlyingReader))
        , ReaderSchema_(schema)
        , RowReorderer_(TNameTable::FromSchema(schema), keyColumns)
        , MemoryPool_(TSchemafulReaderAdapterPoolTag())
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();
        MemoryPool_.Clear();

        if (ErrorPromise_.IsSet()) {
            return true;
        }

        auto hasMore = UnderlyingReader_->Read(rows);
        if (rows->empty()) {
            return hasMore;
        }

        YCHECK(hasMore);
        auto& rows_ = *rows;

        try {
            for (int i = 0; i < rows->size(); ++i) {
                if (!rows_[i]) {
                    continue;
                }

                auto row = RowReorderer_.ReorderKey(rows_[i], &MemoryPool_);
                for (int valueIndex = 0; valueIndex < ReaderSchema_.Columns().size(); ++valueIndex) {
                    const auto& value = row[valueIndex];
                    ValidateDataValue(value);
                    // XXX(babenko)
                    // The underlying schemaless reader may unpack typed scalar values even
                    // if the schema has "any" type. For schemaful reader, this is not an expected behavior
                    // so we have to convert such values back into YSON.
                    // Cf. YT-5396
                    if (ReaderSchema_.Columns()[valueIndex].GetPhysicalType() == EValueType::Any &&
                        value.Type != EValueType::Any &&
                        value.Type != EValueType::Null)
                    {
                        row[valueIndex] = MakeAnyFromScalar(value);
                    } else {
                        ValidateValueType(value, ReaderSchema_, valueIndex, /*typeAnyAcceptsAllValues*/ false);
                    }
                }
                rows_[i] = row;
            }
        } catch (const std::exception& ex) {
            rows_.clear();
            ErrorPromise_.Set(ex);
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (ErrorPromise_.IsSet()) {
            return ErrorPromise_.ToFuture();
        } else {
            return UnderlyingReader_->GetReadyEvent();
        }
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

private:
    const ISchemalessReaderPtr UnderlyingReader_;
    const TTableSchema ReaderSchema_;

    TSchemalessRowReorderer RowReorderer_;
    TChunkedMemoryPool MemoryPool_;
    TBlobOutput ValueBuffer_;

    TPromise<void> ErrorPromise_ = NewPromise<void>();


    TUnversionedValue MakeAnyFromScalar(const TUnversionedValue& value)
    {
        ValueBuffer_.Clear();
        TBufferedBinaryYsonWriter writer(&ValueBuffer_);
        switch (value.Type) {
            case EValueType::Int64:
                writer.OnInt64Scalar(value.Data.Int64);
                break;
            case EValueType::Uint64:
                writer.OnUint64Scalar(value.Data.Uint64);
                break;
            case EValueType::Double:
                writer.OnDoubleScalar(value.Data.Double);
                break;
            case EValueType::Boolean:
                writer.OnBooleanScalar(value.Data.Boolean);
                break;
            case EValueType::String:
                writer.OnStringScalar(TStringBuf(value.Data.String, value.Length));
                break;
            case EValueType::Null:
                writer.OnEntity();
                break;
            default:
                Y_UNREACHABLE();
        }
        writer.Flush();
        auto ysonSize = ValueBuffer_.Size();
        auto* ysonBuffer = MemoryPool_.AllocateUnaligned(ysonSize);
        ::memcpy(ysonBuffer, ValueBuffer_.Begin(), ysonSize);
        return MakeUnversionedAnyValue(TStringBuf(ysonBuffer, ysonSize), value.Id);
    }
};

DEFINE_REFCOUNTED_TYPE(TSchemafulReaderAdapter)

ISchemafulReaderPtr CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter)
{
    TKeyColumns keyColumns;
    for (const auto& columnSchema : schema.Columns()) {
        keyColumns.push_back(columnSchema.Name());
    }

    auto nameTable = TNameTable::FromSchema(schema);
    auto underlyingReader = createReader(
        nameTable,
        columnFilter.All ? TColumnFilter(schema.Columns().size()) : columnFilter);

    auto result = New<TSchemafulReaderAdapter>(
        underlyingReader,
        schema,
        keyColumns);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
