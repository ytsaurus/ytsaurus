#pragma once

#include "common.h"

#include "../misc/property.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NChunkServer {

// TODO: get rid
using NTransaction::TTransactionId;
using NTransaction::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////
// TODO: move implementation to cpp

class TChunk
{
    DECLARE_BYVAL_RO_PROPERTY(Id, TChunkId);
    DECLARE_BYVAL_RW_PROPERTY(ChunkListId, TChunkListId);
    DECLARE_BYVAL_RW_PROPERTY(Size, i64);
    DECLARE_BYREF_RO_PROPERTY(Locations, yvector<THolderId>);

public:
    static const i64 UnknownSize = -1;

    TChunk(const TChunkId& id)
        : Id_(id)
        , Size_(UnknownSize)
        , RefCounter(0)
    { }

    TAutoPtr<TChunk> Clone() const
    {
        return new TChunk(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, ChunkListId_);
        ::Save(output, Size_);
        ::Save(output, Locations_);
        ::Save(output, RefCounter);
    }

    static TAutoPtr<TChunk> Load(const TChunkId& id, TInputStream* input)
    {
        TAutoPtr<TChunk> chunk = new TChunk(id);
        ::Load(input, chunk->ChunkListId_);
        ::Load(input, chunk->Size_);
        ::Load(input, chunk->Locations_);
        ::Load(input, chunk->RefCounter);
        return chunk;
    }


    void AddLocation(THolderId holderId)
    {
        Locations_.push_back(holderId);
    }

    void RemoveLocation(THolderId holderId)
    {
        auto it = Find(Locations_.begin(), Locations_.end(), holderId);
        YASSERT(it != Locations_.end());
        Locations_.erase(it);
    }


    i32 Ref()
    {
        return ++RefCounter;
    }

    i32 Unref()
    {
        return --RefCounter;
    }

private:
    i32 RefCounter;

    TChunk(const TChunk& other)
        : Id_(other.Id_)
        , ChunkListId_(other.ChunkListId_)
        , Size_(other.Size_)
        , Locations_(other.Locations_)
        , RefCounter(other.RefCounter)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
