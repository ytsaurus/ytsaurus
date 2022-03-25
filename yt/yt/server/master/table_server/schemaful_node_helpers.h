#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

std::pair<const NTableClient::TTableSchema*, NTableClient::TTableSchemaPtr> ProcessSchemafulNodeAttributes(
    const NYTree::IAttributeDictionaryPtr& combinedAttributes,
    bool dynamic,
    bool chaos,
    const NCellMaster::TDynamicClusterConfigPtr& dynamicConfig,
    const TTableManagerPtr& tableManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
