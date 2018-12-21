#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/hydra/composite_automaton.h>

#include <yt/server/node_tracker_server/public.h>

#include <yt/server/object_server/public.h>

#include <yt/server/security_server/public.h>

#include <yt/server/table_server/public.h>

#include <yt/server/tablet_server/public.h>

#include <yt/server/transaction_server/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
     
#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
