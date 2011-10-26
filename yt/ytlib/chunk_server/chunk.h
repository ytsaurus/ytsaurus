#pragma once

#include "common.h"

#include "../misc/property.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NChunkServer {

using NTransaction::TTransactionId;
using NTransaction::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////
// TODO: move implementation to cpp

class TChunk
{
    DECLARE_BYVAL_RO_PROPERTY(Id, TChunkId);
    DECLARE_BYVAL_RW_PROPERTY(TransactionId, TTransactionId);
    DECLARE_BYVAL_RW_PROPERTY(Size, i64);
    DECLARE_BYREF_RO_PROPERTY(Locations, yvector<THolderId>);

public:
    static const i64 UnknownSize = -1;

    TChunk(
        const TChunkId& id,
        const TTransactionId& transactionId)
        : Id_(id)
        , TransactionId_(transactionId)
        , Size_(UnknownSize)
    { }

    TAutoPtr<TChunk> Clone() const
    {
        return new TChunk(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id_);
        ::Save(output, TransactionId_);
        ::Save(output, Size_);
        ::Save(output, Locations_);
    }

    static TAutoPtr<TChunk> Load(TInputStream* input)
    {
        TChunkId id;
        NTransaction::TTransactionId transactionId;
        ::Load(input, id);
        ::Load(input, transactionId);
        auto* chunk = new TChunk(id, transactionId);
        ::Load(input, chunk->Size_);
        ::Load(input, chunk->Locations_);
        return chunk;
    }

    bool IsVisible(const TTransactionId& transactionId) const
    {
        return
            TransactionId_ != NullTransactionId ||
            TransactionId_ == transactionId;
    }


    void AddLocation(THolderId holderId)
    {
        Locations_.push_back(holderId);
    }

    void RemoveLocation(THolderId holderId)
    {
        auto it = std::find(Locations_.begin(), Locations_.end(), holderId);
        YASSERT(it != Locations_.end());
        Locations_.erase(it);
    }

private:
    TChunk(const TChunk& other)
        : Id_(other.Id_)
        , TransactionId_(other.TransactionId_)
        , Size_(other.Size_)
        , Locations_(other.Locations_)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
