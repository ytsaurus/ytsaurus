#pragma once

#include "common.h"
#include "id.h"

#include <yt/ytlib/misc/property.h>
#include <yt/ytlib/object_server/object.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYREF_RW_PROPERTY(yvector<TChunkTreeId>, ChildrenIds);

public:
    TChunkList(const TChunkListId& id);

    TAutoPtr<TChunkList> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TChunkList> Load(const TChunkListId& id, TInputStream* input);

private:
    i32 RefCounter;

    TChunkList(const TChunkList& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
