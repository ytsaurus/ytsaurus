#pragma once

#include "private.h"
#include "block_store.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/logging/logger.h>

#include <vector>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Reads the metadata of a device snapshot from a (caller-fetched) load spec.
/*!
 *  The block payloads themselves are NOT fetched -- only the hunk references stored in the table's
 *  rows are read.
 *
 *  Blocking; run it on a background invoker.
 */
std::vector<TSnapshotBlock> ReadJournalSnapshot(
    const NApi::NNative::IClientPtr& client,
    const TSnapshotLoadSpec& loadSpec,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
