#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

//! Tracks memory that is used by blocks.
class IBlockTracker
    : public TRefCounted
{
public:
    //! Adds RAII tracker to the block to track its lifetime.
    virtual NChunkClient::TBlock RegisterBlock(NChunkClient::TBlock block) = 0;

    //! Functions to increment/decrement internal usage counter.
    virtual void AcquireCategory(TRef ref, NNodeTrackerClient::EMemoryCategory category) = 0;
    virtual void ReleaseCategory(TRef ref, NNodeTrackerClient::EMemoryCategory category) = 0;

    //! Methods for TBlockHolder.
    virtual void OnUnregisterBlock(TRef ref) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockTracker);

IBlockTrackerPtr CreateBlockTracker(INodeMemoryTrackerPtr tracker);

/////////////////////////////////////////////////////////////////////////////

//! Registers block and adds category [if category is not nullopt]
NChunkClient::TBlock AttachCategory(
    NChunkClient::TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category);

TSharedRef AttachCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category);

/////////////////////////////////////////////////////////////////////////////

/*!
 *  Destroyes block holder and puts a new one
 *  with RAII category usage guard on its place.
 */
NChunkClient::TBlock ResetCategory(
    NChunkClient::TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category);

TSharedRef ResetCategory(
    TSharedRef block,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category);

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
