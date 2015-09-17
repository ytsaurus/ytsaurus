#include "stdafx.h"
#include "store.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TDynamicMemoryStorePtr IStore::AsDynamicMemory()
{
    auto* result = dynamic_cast<TDynamicMemoryStore*>(this);
    YCHECK(result);
    return result;
}

TChunkStorePtr IStore::AsChunk()
{
    auto* result = dynamic_cast<TChunkStore*>(this);
    YCHECK(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TStoreIdFormatter::operator()(
    TStringBuilder* builder,
    const IStorePtr& store) const
{
    FormatValue(builder, store->GetId(), STRINGBUF("v"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
