#pragma once

#include "common.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

struct TChunk
{
    typedef yvector<THolderId> TLocations;

    static const i64 UnknownSize = -1;

    TChunk(
        const TChunkId& id,
        const TTransactionId& transactionId)
        : Id(id)
        , Size(UnknownSize)
        , TransactionId(transactionId)
    { }

    TChunk(const TChunk& other)
        : Id(other.Id)
        , Size(other.Size)
        , TransactionId(other.TransactionId)
        , Locations(other.Locations)
    { }

    TAutoPtr<TChunk> Clone() const
    {
        return new TChunk(*this);
    }

    bool IsVisible(const NTransaction::TTransactionId& transactionId) const
    {
        return
            TransactionId != NTransaction::TTransactionId() ||
            TransactionId == transactionId;
    }


    void AddLocation(THolderId holderId)
    {
        if (!IsIn(Locations, holderId)) {
            Locations.push_back(holderId);
        }
    }

    void RemoveLocation(THolderId holderId)
    {
        auto it = Find(Locations.begin(), Locations.end(), holderId);
        if (it != Locations.end()) {
            Locations.erase(it);
        }
    }

    TChunkId Id;
    i64 Size;
    NTransaction::TTransactionId TransactionId;
    TLocations Locations;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
