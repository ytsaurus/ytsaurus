#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceOptions)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockFlusherConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockCompactorConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

//! Exclusive upper bound on a device's block count.
constexpr i64 MaxBlocksPerDevice = 1LL << 30;

//! Size of one block map entry, which the device keeps per device block for its whole lifetime.
constexpr i64 BlockMapEntrySize = sizeof(ui64);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
