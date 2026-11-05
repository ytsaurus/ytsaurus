#pragma once

#include <yt/yt/core/ytree/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Leave only instances of the latest operation incarnation to avoid communication
//! between cliques from different operations
THashMap<std::string, NYTree::IAttributeDictionaryPtr> FilterInstancesByIncarnation(const THashMap<std::string, NYTree::IAttributeDictionaryPtr>& instances);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
