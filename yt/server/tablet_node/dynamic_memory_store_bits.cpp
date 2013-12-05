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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
