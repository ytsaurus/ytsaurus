#include "stdafx.h"
#include "chunk_store.h"

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/versioned_reader.h>

namespace NYT {
namespace NTabletNode {

using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(const TStoreId& id)
    : Id_(id)
    , State_(EStoreState::Persistent)
{
    YCHECK(
        TypeFromId(Id_) == EObjectType::Chunk ||
        TypeFromId(Id_) == EObjectType::ErasureChunk);
}

TChunkStore::~TChunkStore()
{ }

TStoreId TChunkStore::GetId() const
{
    return Id_;
}

EStoreState TChunkStore::GetState() const
{
    return State_;
}

void TChunkStore::SetState(EStoreState state)
{
    State_ = state;
}

IVersionedReaderPtr TChunkStore::CreateReader(
    TKey lowerKey,
    TKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

