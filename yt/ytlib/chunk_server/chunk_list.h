#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
{
    DECLARE_BYVAL_RO_PROPERTY(TChunkListId, Id);
    DECLARE_BYREF_RW_PROPERTY(yvector<TChunkId>, ChunkIds);
    DECLARE_BYVAL_RW_PROPERTY(i32, ReplicaCount);

public:
    TChunkList(const TChunkListId& id)
        : Id_(id)
        , ReplicaCount_(3)
        , RefCounter(0)
    { }

    TAutoPtr<TChunkList> Clone() const
    {
        return new TChunkList(*this);
    }

    void Save(TOutputStream* output) const
    {
        //::Save(output, Id_);
        ::Save(output, ChunkIds_);
        ::Save(output, ReplicaCount_);
        ::Save(output, RefCounter);
    }

    static TAutoPtr<TChunkList> Load(const TChunkListId& id, TInputStream* input)
    {
        TAutoPtr<TChunkList> chunkList = new TChunkList(id);
        ::Load(input, chunkList->ChunkIds_);
        ::Load(input, chunkList->ReplicaCount_);
        ::Load(input, chunkList->RefCounter);
        return chunkList;
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

    TChunkList(const TChunkList& other)
        : Id_(other.Id_)
        , ChunkIds_(other.ChunkIds_)
        , ReplicaCount_(other.ReplicaCount_)
        , RefCounter(other.RefCounter)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
