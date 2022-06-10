#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Tracks memory used by blocks.
 *
 * Memory is split into different memory categories.
 *
 * The purpose of block tracker is to track categories attached to blocks and
 * account block only once in memory usage tracker.
 *
 * Accounted category is picked as follows:
 * - UnknownBlocks if no category is attached
 * - Attached category if either exactly one category is attached
 * - Attached category if exactly two categories are attached and another one is 'BlockCache'
 *     (thus different subsystems can borrow memory from cache)
 * - Mixed otherwise
 *
 * Category tracking is implemented by additional holders in TSharedRef.
 * User should call AttachCategory or ResetCategory to receive a new reference with holder.
 * When the reference is destroyed holder releases category from block.
 */
struct IBlockTracker
    : public TRefCounted
{
    //! Registers block if it is not registered yet.
    //! Returns enhanced reference which will unregister itself from block tracker when destroyed
    virtual TSharedRef RegisterBlock(TSharedRef block) = 0;

    //! Methods for incrementing/decrementing internal usage counter.
    virtual void AcquireCategory(TRef ref, EMemoryCategory category) = 0;
    virtual void ReleaseCategory(TRef ref, EMemoryCategory category) = 0;

    //! Method used by BlockHolder.
    virtual void OnUnregisterBlock(TRef ref) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockTracker);

////////////////////////////////////////////////////////////////////////////////

/*!
 * Register block in block tracker if necessary.
 * Attaches memory category to registered block.
 * Retruns reference with specified category attached.
 * All other holders in block ref remain in the reference.
 */
TSharedRef AttachCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category);

////////////////////////////////////////////////////////////////////////////////

/*!
 * Register block in block tracker if necessary.
 * Attaches memory category to registered block.
 * Retruns reference with specified category attached.
 * All other holders in block ref are dropped.
 */
TSharedRef ResetCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
