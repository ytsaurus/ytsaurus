#pragma once

#include "private.h"

#include <yt/core/misc/phoenix.h>

#include <yt/client/table_client/serialize.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion();
bool ValidateSnapshotVersion(int version);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESnapshotVersion,
    ((JobSplitterSpeculative) (300104))
    ((InputOutputTableLock)   (300105))
    ((PrepareRootFSDuration)  (300106))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
