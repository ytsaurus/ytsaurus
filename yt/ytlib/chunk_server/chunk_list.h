#pragma once

#include "common.h"
#include "id.h"

#include <ytlib/misc/property.h>
#include <ytlib/object_server/object_detail.h>

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
    TChunkList(const TChunkList& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
