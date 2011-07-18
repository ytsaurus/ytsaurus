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

    TChunk(
        TChunkId id,
        i64 size = UnknownSize)
        : Id(id)
        , Size(size)
    { }

    TChunkId GetId() const
    {
        return Id;
    }

    NTransaction::TTransactionId GetTransactionId() const
    {
        return TransactionId;
    }

    void SetTransactionId(NTransaction::TTransactionId transactionId)
    {
        TransactionId = transactionId;
    }

    void ResetTransactionId()
    {
        TransactionId = NTransaction::TTransactionId();
    }

    bool IsVisible(NTransaction::TTransactionId transactionId) const
    {
        return TransactionId != NTransaction::TTransactionId() ||
               TransactionId == transactionId;
    }

    i64 GetSize() const
    {
        return Size;
    }

    TLocations& Locations()
    {
        return Locations_;
    }

private:
    TChunkId Id;
    i64 Size;

    NTransaction::TTransactionId TransactionId;
    TLocations Locations_; 

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
