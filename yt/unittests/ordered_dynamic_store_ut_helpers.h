#include "dynamic_store_ut_helpers.h"

#include <yt/ytlib/table_client/schemaful_reader.h>

namespace NYT {
namespace NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreTestBase
    : public TDynamicStoreTestBase
{
protected:
    virtual TTableSchema GetSchema() const override
    {
        return TTableSchema({
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String)
        });
    }

    TUnversionedOwningRow GetRow(IOrderedStorePtr store, i64 index)
    {
        auto reader = store->CreateReader(index, index + 1, GetSchema(), TWorkloadDescriptor());

        std::vector<TUnversionedRow> rows;
        rows.reserve(1);

        EXPECT_TRUE(reader->Read(&rows));
        EXPECT_EQ(1, rows.size());
        return TUnversionedOwningRow(rows[0]);
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

