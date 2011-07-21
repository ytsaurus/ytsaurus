#pragma once

#include "common.h"

#include "../transaction/common.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunk> TPtr;
    typedef yvector<int> TLocations;

    static const i64 UnknownSize = -1;

    TChunk(TChunkId id)
        : Id(id)
        , Size_(UnknownSize)
    { }

    TChunkId GetId() const
    {
        return Id;
    }

    TTransactionId& TransactionId()
    {
        return TransactionId_;
    }

    bool IsVisible(NTransaction::TTransactionId transactionId) const
    {
        return TransactionId_ != NTransaction::TTransactionId() ||
               TransactionId_ == transactionId;
    }

    i64& Size()
    {
        return Size_;
    }

    TLocations& Locations()
    {
        return Locations_;
    }

private:
    TChunkId Id;
    i64 Size_;
    NTransaction::TTransactionId TransactionId_;
    TLocations Locations_; 

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
