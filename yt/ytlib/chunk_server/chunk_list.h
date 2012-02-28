#pragma once

#include "common.h"
#include "id.h"
#include "chunk_statistics.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/misc/property.h>
#include <ytlib/object_server/object_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkList
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYREF_RW_PROPERTY(yvector<TChunkTreeId>, ChildrenIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkListId>, ParentIds);
    DEFINE_BYREF_RW_PROPERTY(TChunkStatistics, Statistics);

public:
    TChunkList(const TChunkListId& id);

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input, const NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
