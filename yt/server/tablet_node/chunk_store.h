#pragma once

#include "public.h"
#include "store.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore
    : public IStore
{
public:
    explicit TChunkStore(const TStoreId& id);
    ~TChunkStore();

    // IStore implementation.
    virtual TStoreId GetId() const override;

    virtual EStoreState GetState() const override;
    virtual void SetState(EStoreState state) override;

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TKey lowerKey,
        NVersionedTableClient::TKey upperKey,
        TTimestamp timestamp,
        const NApi::TColumnFilter& columnFilter) override;

private:
    TStoreId Id_;
    
    EStoreState State_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
