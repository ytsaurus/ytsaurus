#include "schema_ordering_writer.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

namespace NYT::NRowIOBenchmark {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// The low-level columnar writer expects schema-ordered rows. In jobs this
// normalization is performed by the multi-chunk writer, before chunk encoding.
class TSchemaOrderingWriter
    : public IUnversionedWriter
{
public:
    explicit TSchemaOrderingWriter(IUnversionedWriterPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        RowBuffer_->Clear();
        std::vector<TUnversionedRow> orderedRows;
        orderedRows.reserve(rows.Size());
        int columnCount = GetSchema()->GetColumnCount();
        std::vector<bool> columnSeenFlags(columnCount);
        for (auto row : rows) {
            auto orderedRow = RowBuffer_->AllocateUnversioned(columnCount);
            for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
                orderedRow[columnIndex] = MakeUnversionedNullValue(columnIndex);
                columnSeenFlags[columnIndex] = false;
            }
            for (const auto& value : row) {
                // CreateWriter initializes the name table in schema order.
                if (value.Id >= columnCount) {
                    THROW_ERROR_EXCEPTION(
                        NTableClient::EErrorCode::SchemaViolation,
                        "Unexpected column in row")
                        .With("column", GetNameTable()->GetName(value.Id));
                }
                if (columnSeenFlags[value.Id]) {
                    THROW_ERROR_EXCEPTION(
                        NTableClient::EErrorCode::SchemaViolation,
                        "Duplicate column in row")
                        .With("column", GetNameTable()->GetName(value.Id));
                }
                columnSeenFlags[value.Id] = true;
                orderedRow[value.Id] = value;
            }
            for (const auto& value : orderedRow) {
                ValidateValueType(value, *GetSchema(), value.Id, /*typeAnyAcceptsAllValues*/ true);
            }
            orderedRows.push_back(orderedRow);
        }
        return Underlying_->Write(orderedRows);
    }

    TFuture<void> GetReadyEvent() override
    {
        return Underlying_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        return Underlying_->Close();
    }

    std::optional<TRowsDigest> GetDigest() const override
    {
        return Underlying_->GetDigest();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Underlying_->GetNameTable();
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Underlying_->GetSchema();
    }

private:
    const IUnversionedWriterPtr Underlying_;
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
};

DEFINE_REFCOUNTED_TYPE(TSchemaOrderingWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemaOrderingWriter(IUnversionedWriterPtr underlying)
{
    return New<TSchemaOrderingWriter>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
