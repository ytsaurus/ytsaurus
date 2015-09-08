#pragma once

#include "public.h"
#include "automaton.h"

#include <server/hydra/composite_automaton.h>

#include <server/node_tracker_server/public.h>

#include <server/object_server/public.h>

#include <server/transaction_server/public.h>

#include <server/chunk_server/public.h>

#include <server/cypress_server/public.h>

#include <server/security_server/public.h>

#include <server/table_server/public.h>

#include <server/tablet_server/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
     
#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
