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
        std::unique_ptr<TWireProtocolReader> reader,
        const TTableSchema& schema,
        std::vector<TUnversionedRow> rows)
        : Reader_(std::move(reader))
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
    std::unique_ptr<TWireProtocolReader> Reader_;
    TTableSchema Schema_;
    std::vector<TUnversionedRow> Rows_;

};

IRowsetPtr CreateRowset(
    std::unique_ptr<TWireProtocolReader> reader,
    const TTableSchema& schema,
    std::vector<TUnversionedRow> rows)
{   
    YCHECK(reader);

    return New<TRowset>(
        std::move(reader),
        schema,
        std::move(rows));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

