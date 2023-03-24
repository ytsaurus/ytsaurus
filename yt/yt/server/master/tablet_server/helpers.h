#pragma once

#include "public.h"

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

bool IsHunkChunk(const TTabletBase* tablet, const NChunkServer::TChunkTree* child);

bool IsDynamicStoreReadEnabled(
    const NTableServer::TTableNode* table,
    const TDynamicTabletManagerConfigPtr& dynamicConfig);

void ParseTabletRange(TTabletOwnerBase* owner, int* first, int* last);
void ParseTabletRangeOrThrow(const TTabletOwnerBase* table, int* first, int* last);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
