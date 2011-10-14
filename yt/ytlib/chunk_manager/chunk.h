#pragma once

#include "common.h"
#include <util/ysaveload.h>

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
        , TransactionId(transactionId)
        , Size(UnknownSize)
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

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id);
        ::Save(output, TransactionId);
        ::Save(output, Size);
        ::Save(output, Locations);
    }

    static TAutoPtr<TChunk> Load(TInputStream* input)
    {
        TChunkId id;
        NTransaction::TTransactionId transactionId;
        ::Load(input, id);
        ::Load(input, transactionId);
        auto* chunk = new TChunk(id, transactionId);
        ::Load(input, chunk->Size);
        ::Load(input, chunk->Locations);
        return chunk;
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
    NTransaction::TTransactionId TransactionId;
    i64 Size;
    TLocations Locations;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
