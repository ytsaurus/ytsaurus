#pragma once

#include "private.h"

#include <yt/core/misc/phoenix.h>

#include <yt/client/table_client/serialize.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
