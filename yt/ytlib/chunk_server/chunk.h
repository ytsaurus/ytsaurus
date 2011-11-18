#pragma once

#include "common.h"

#include "../misc/property.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
{
    DECLARE_BYVAL_RO_PROPERTY(TChunkId, Id);
    DECLARE_BYVAL_RW_PROPERTY(TChunkListId, ChunkListId);
    DECLARE_BYVAL_RW_PROPERTY(i64, Size);
    DECLARE_BYREF_RO_PROPERTY(yvector<THolderId>, Locations);

public:
    static const i64 UnknownSize = -1;

    TChunk(const TChunkId& id);

    TAutoPtr<TChunk> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TChunk> Load(const TChunkId& id, TInputStream* input);

    void AddLocation(THolderId holderId);
    void RemoveLocation(THolderId holderId);

    i32 Ref();
    i32 Unref();

private:
    i32 RefCounter;

    TChunk(const TChunk& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
