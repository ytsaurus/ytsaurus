#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJournalBlockDevice)

DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceOptions)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockFlusherConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceConfig)

//! The load side of a device snapshot: a resolved read spec for the snapshot table (fetched by the caller).
using TSnapshotLoadSpec = NTableClient::TTableReadSpec;

//! The save side of a device snapshot: the resolved target table.
using TSnapshotSaveSpec = NChunkClient::TUserObject;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
