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

class TRowset
    : public IRowset
{
public:
    TRowset(
        const TTableSchema& schema,
        TSharedRange<TUnversionedRow> rows)
        : Schema_(schema)
        , Rows_(std::move(rows))
    { }

    virtual const TTableSchema& Schema() const override
    {
        return Schema_;
    }

    virtual TRange<TUnversionedRow> GetRows() const override
    {
        return Rows_;
    }

private:
    const TTableSchema Schema_;
    const TSharedRange<TUnversionedRow> Rows_;

};

IRowsetPtr CreateRowset(
    const TTableSchema& schema,
    TSharedRange<TUnversionedRow> rows)
{   
    return New<TRowset>(schema, std::move(rows));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowsetWriter
    : public IRowset
    , public ISchemafulWriter
{
public:
    explicit TSchemafulRowsetWriter(const TTableSchema& schema)
        : Schema_(schema)
    { }

    virtual const TTableSchema& Schema() const override
    {
        return Schema_;
    }

    virtual TRange<TUnversionedRow> GetRows() const override
    {
        return MakeRange(Rows_);
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

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
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
    const TTableSchema Schema_;

    TPromise<IRowsetPtr> Result_ = NewPromise<IRowsetPtr>();

    struct TSchemafulRowsetWriterBufferTag
    { };

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSchemafulRowsetWriterBufferTag());
    std::vector<TUnversionedRow> Rows_;

};

std::tuple<ISchemafulWriterPtr, TFuture<IRowsetPtr>> CreateSchemafulRowsetWriter(const TTableSchema& schema)
{
    auto writer = New<TSchemafulRowsetWriter>(schema);
    return std::make_tuple(writer, writer->GetResult());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

