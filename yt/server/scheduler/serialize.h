#pragma once

#include "private.h"

#include <yt/core/misc/phoenix.h>

#include <yt/ytlib/table_client/serialize.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
