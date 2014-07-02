#pragma once

#include "private.h"

#include <core/misc/phoenix.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

typedef NPhoenix::TSaveContext TSaveContext;
typedef NPhoenix::TLoadContext TLoadContext;
typedef NPhoenix::TPersistenceContext TPersistenceContext;
typedef NPhoenix::IPersistent IPersistent;

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
