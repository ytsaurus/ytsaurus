#include "stdafx.h"
#include "rowset.h"

#include <core/misc/chunked_memory_pool.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <ytlib/tablet_client/wire_protocol.h>

namespace NYT {
namespace NApi {

using namespace NTabletClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

class TRowset
    : public IRowset
{
public:
    TRowset(
        std::vector<std::unique_ptr<TWireProtocolReader>> readers,
        const TTableSchema& schema,
        std::vector<TUnversionedRow> rows)
        : Readers_(std::move(readers))
        , Schema_(schema)
        , Rows_(std::move(rows))
    { }

    virtual const TTableSchema& Schema() const override
    {
        return Schema_;
    }

    virtual const std::vector<TUnversionedRow>& Rows() const override
    {
        return Rows_;
    }

private:
    std::vector<std::unique_ptr<TWireProtocolReader>> Readers_;
    TTableSchema Schema_;
    std::vector<TUnversionedRow> Rows_;

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

static const auto PresetResult = MakeFuture(TError());

class TSchemedRowsetWriter
    : public ISchemedWriter
    , public IRowset
{
public:
    TSchemedRowsetWriter()
        : Result_(NewPromise<TErrorOr<IRowsetPtr>>())
    { }

    TPromise<TErrorOr<IRowsetPtr>> GetResult() const
    {
        return Result_;
    }

    // IRowset implementation.
    virtual const TTableSchema& Schema() const override
    {
        return Schema_;
    }

    virtual const std::vector<TUnversionedRow>& Rows() const override
    {
        return Rows_;
    }

    // ISchemedWriter implementation.
    virtual TAsyncError Open(
        const TTableSchema& schema,
        const TNullable<TKeyColumns>& /*keyColumns*/) override
    {
        Schema_ = schema;
        return PresetResult;
    }


    virtual TAsyncError Close() override
    {
        Result_.Set(TError());
        return PresetResult;
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        for (auto row : rows) {
            Rows_.push_back(row.Capture(&AlignedPool_, &UnalignedPool_));
        }
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return PresetResult;
    }

private:
    TPromise<TErrorOr<IRowsetPtr>> Result_;

    TTableSchema Schema_;
    std::vector<TUnversionedRow> Rows_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;

};

std::tuple<ISchemedWriterPtr, TPromise<TErrorOr<IRowsetPtr>>> CreateSchemedRowsetWriter()
{
    auto writer = New<TSchemedRowsetWriter>();
    return std::make_tuple(writer, writer->GetResult());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

