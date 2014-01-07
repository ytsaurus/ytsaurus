#include "stdafx.h"
#include "persistent_store.h"

#include <ytlib/new_table_client/versioned_reader.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NApi;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TPersistentStore::TPersistentStore(const TChunkId& chunkId)
{ }

TPersistentStore::~TPersistentStore()
{ }

IVersionedReaderPtr TPersistentStore::CreateReader(
    TKey lowerKey,
    TKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    return nullptr;
}

bool TPersistentStore::IsPersistent() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

