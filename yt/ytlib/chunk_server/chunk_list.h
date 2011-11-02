#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
{
    DECLARE_BYVAL_RO_PROPERTY(Id, TChunkListId);
    DECLARE_BYREF_RW_PROPERTY(Chunks, yvector<TChunkId>);
    DECLARE_BYVAL_RW_PROPERTY(ReplicaCount, i32);

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
        ::Save(output, Id_);
        ::Save(output, Chunks_);
        ::Save(output, ReplicaCount_);
        ::Save(output, RefCounter);
    }

    static TAutoPtr<TChunkList> Load(TInputStream* input)
    {
        TChunkListId id;
        ::Load(input, id);
        TAutoPtr<TChunkList> chunkList = new TChunkList(id);
        ::Load(input, chunkList->Chunks_);
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
        , Chunks_(other.Chunks_)
        , ReplicaCount_(other.ReplicaCount_)
        , RefCounter(other.RefCounter)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
