#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../misc/serialize.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
{
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TChunkListId, ChunkListId);
    DEFINE_BYVAL_RW_PROPERTY(i64, Size);
    DEFINE_BYVAL_RW_PROPERTY(TSharedRef, MasterMeta);
    DEFINE_BYREF_RO_PROPERTY(yvector<THolderId>, Locations);

public:
    static const i64 UnknownSize = -1;

    TChunk(const NChunkClient::TChunkId& id);

    TAutoPtr<TChunk> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TChunk> Load(const NChunkClient::TChunkId& id, TInputStream* input);

    void AddLocation(THolderId holderId);
    void RemoveLocation(THolderId holderId);

    i32 Ref();
    i32 Unref();
    i32 GetRefCounter() const;

    template <class TMeta>
    TMeta DeserializeMasterMeta() const
    {
        TMeta meta;
        if (!DeserializeProtobuf(&meta, MasterMeta_)) {
            NLog::TLogger& Logger = ChunkServerLogger;
            LOG_FATAL("Error deserializing chunk meta (ChunkId: %s, TypeName: %s)",
                ~Id_.ToString(),
                meta.GetTypeName().c_str());
        }
        return meta;
    }

private:
    i32 RefCounter;

    TChunk(const TChunk& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
