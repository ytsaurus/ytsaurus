#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceOptions)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockFlusherConfig)
DECLARE_REFCOUNTED_STRUCT(TJournalBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
