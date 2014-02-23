#include "stdafx.h"
#include "rowset.h"

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>

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

} // namespace NApi
} // namespace NYT

