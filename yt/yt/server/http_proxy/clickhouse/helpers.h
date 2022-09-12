#pragma once

#include <yt/yt/core/ytree/public.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

//! Leave only instances of the latest operation incarnation to avoid communication
//! between cliques from different operations
THashMap<TString, NYTree::IAttributeDictionaryPtr> FilterInstancesByIncarnation(const THashMap<TString, NYTree::IAttributeDictionaryPtr>& instances);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
