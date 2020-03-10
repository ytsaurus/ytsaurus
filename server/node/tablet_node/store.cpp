#include "store.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "ordered_chunk_store.h"
#include "ordered_dynamic_store.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Remove dynamic_cast. Return false or nullptr from IStore. Implement corresponding functions in
// Derived classes.
// TODO(lukyan): Remove GetType usages.

bool IStore::IsDynamic() const
{
    auto type = GetType();
    return type == EStoreType::SortedDynamic;
}

IDynamicStorePtr IStore::AsDynamic()
{
    auto* result = dynamic_cast<IDynamicStore*>(this);
    YT_VERIFY(result);
    return result;
}

bool IStore::IsChunk() const
{
    auto type = GetType();
    return type == EStoreType::SortedChunk;
}

IChunkStorePtr IStore::AsChunk()
{
    auto* result = dynamic_cast<IChunkStore*>(this);
    YT_VERIFY(result);
    return result;
}

bool IStore::IsSorted() const
{
    auto type = GetType();
    return type == EStoreType::SortedDynamic ||
           type == EStoreType::SortedChunk;
}

ISortedStorePtr IStore::AsSorted()
{
    auto* result = dynamic_cast<ISortedStore*>(this);
    YT_VERIFY(result);
    return result;
}

TSortedDynamicStorePtr IStore::AsSortedDynamic()
{
    auto* result = dynamic_cast<TSortedDynamicStore*>(this);
    YT_VERIFY(result);
    return result;
}

TSortedChunkStorePtr IStore::AsSortedChunk()
{
    auto* result = dynamic_cast<TSortedChunkStore*>(this);
    YT_VERIFY(result);
    return result;
}

bool IStore::IsOrdered() const
{
    auto type = GetType();
    return type == EStoreType::OrderedDynamic ||
           type == EStoreType::OrderedChunk;
}

IOrderedStorePtr IStore::AsOrdered()
{
    auto* result = dynamic_cast<IOrderedStore*>(this);
    YT_VERIFY(result);
    return result;
}

TOrderedDynamicStorePtr IStore::AsOrderedDynamic()
{
    auto* result = dynamic_cast<TOrderedDynamicStore*>(this);
    YT_VERIFY(result);
    return result;
}
TOrderedChunkStorePtr IStore::AsOrderedChunk()
{
    auto* result = dynamic_cast<TOrderedChunkStore*>(this);
    YT_VERIFY(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TStoreIdFormatter::operator()(TStringBuilderBase* builder, const IStorePtr& store) const
{
    FormatValue(builder, store->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
