#pragma once

#include "public.h"
#include "store.h"

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TPersistentStore
    : public IStore
{
public:
    explicit TPersistentStore(const NChunkClient::TChunkId& chunkId);
    ~TPersistentStore();

    // IStore implementation.
    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TKey lowerKey,
        NVersionedTableClient::TKey upperKey,
        TTimestamp timestamp,
        const NApi::TColumnFilter& columnFilter) override;

    virtual bool IsPersistent() const override;

private:

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
