#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

void ValidateDiskSpace(i64 diskSpace);

template <class T>
THashMap<TString, T> CellTagMapToCellNameMap(const THashMap<NObjectClient::TCellTag, T>& map, const NCellMaster::TMulticellManagerPtr& multicellManager);

template <class T>
THashMap<NObjectClient::TCellTag, T> CellNameMapToCellTagMapOrThrow(const THashMap<TString, T>& map, const NCellMaster::TMulticellManagerPtr& multicellManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
