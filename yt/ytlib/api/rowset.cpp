#include "rowset.h"

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NApi {

using namespace NTabletClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TRowsetBase
    : public IRowset
{
public:
    // IRowset implementation.
    virtual const TTableSchema& Schema() const override
    {
        return Schema_;
    }

    virtual const std::vector<TUnversionedRow>& Rows() const override
    {
        return Rows_;
    }

protected:
    TTableSchema Schema_;
    std::vector<TUnversionedRow> Rows_;

};

////////////////////////////////////////////////////////////////////////////////

class TRowset
    : public TRowsetBase
{
public:
    TRowset(
        std::vector<std::unique_ptr<TWireProtocolReader>> readers,
        const TTableSchema& schema,
        std::vector<TUnversionedRow> rows)
        : Readers_(std::move(readers))
    {
        Schema_ = schema;
        Rows_ = std::move(rows);
    }

private:
    std::vector<std::unique_ptr<TWireProtocolReader>> Readers_;

};

IRowsetPtr CreateRowset(
    std::vector<std::unique_ptr<NTabletClient::TWireProtocolReader>> readers,
    const TTableSchema& schema,
    std::vector<TUnversionedRow> rows)
{   
    return New<TRowset>(
        std::move(readers),
        schema,
        std::move(rows));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowsetWriter
    : public TRowsetBase
    , public ISchemafulWriter
{
public:
    TSchemafulRowsetWriter(const TTableSchema& schema)
    {
        Schema_ = schema;
    }

    TFuture<IRowsetPtr> GetResult() const
    {
        return Result_.ToFuture();
    }

    virtual TFuture<void> Close() override
    {
        Result_.Set(IRowsetPtr(this));
        Result_.Reset();
        return VoidFuture;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        for (auto row : rows) {
            Rows_.push_back(RowBuffer_->Capture(row));
        }
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

private:
    TPromise<IRowsetPtr> Result_ = NewPromise<IRowsetPtr>();

    struct TSchemafulRowsetWriterBufferTag
    { };

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSchemafulRowsetWriterBufferTag());

};

std::tuple<ISchemafulWriterPtr, TFuture<IRowsetPtr>> CreateSchemafulRowsetWriter(const TTableSchema& schema)
{
    auto writer = New<TSchemafulRowsetWriter>(schema);
    return std::make_tuple(writer, writer->GetResult());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

