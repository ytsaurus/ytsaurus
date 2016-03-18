#include "dynamic_store_ut_helpers.h"

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
};

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

