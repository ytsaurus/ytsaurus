#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkListId, Id);
    DEFINE_BYREF_RW_PROPERTY(yvector<NChunkClient::TChunkId>, ChunkIds);
    DEFINE_BYVAL_RW_PROPERTY(i32, ReplicaCount);

public:
    TChunkList(const TChunkListId& id);

    TAutoPtr<TChunkList> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TChunkList> Load(const TChunkListId& id, TInputStream* input);


    i32 Ref();
    i32 Unref();
    i32 GetRefCounter() const;

private:
    i32 RefCounter;

    TChunkList(const TChunkList& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
