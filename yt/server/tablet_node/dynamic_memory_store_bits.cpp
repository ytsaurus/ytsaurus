#include "stdafx.h"
#include "dynamic_memory_store_bits.h"
#include "dynamic_memory_store.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TDynamicRowRef::TDynamicRowRef()
{ }

TDynamicRowRef::TDynamicRowRef(const TDynamicRowRef& other)
    : Store(other.Store)
    , Row(other.Row)
{ }

TDynamicRowRef::TDynamicRowRef(TDynamicRowRef&& other)
    : Store(std::move(other.Store))
    , Row(other.Row)
{ }

TDynamicRowRef::TDynamicRowRef(TDynamicMemoryStorePtr store, TDynamicRow row)
    : Store(std::move(store))
    , Row(row)
{ }

TDynamicRowRef::~TDynamicRowRef()
{ }

void swap(TDynamicRowRef& lhs, TDynamicRowRef& rhs)
{
    using std::swap;
    swap(lhs.Store, rhs.Store);
    swap(lhs.Row, rhs.Row);
}

TDynamicRowRef& TDynamicRowRef::operator = (TDynamicRowRef other)
{
    swap(*this, other);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
