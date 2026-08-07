#pragma once

#include "private.h"
#include "block_store.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! The device's geometry, persisted to the snapshot table's @device_params attribute so a device
//! restored from it (see TRestoredDeviceCreationDescriptor) can reconstruct it. The remaining device
//! options (account, medium, replication/quorum) are derived from the table's own Cypress attributes.
struct TSerializableDeviceParams
    : public NYTree::TYsonStructLite
{
    i64 DeviceSize = 0;
    i64 BlockSize = 0;

    REGISTER_YSON_STRUCT_LITE(TSerializableDeviceParams);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Writes a journal device snapshot into the (already created and resolved) table |userObject|.
/*!
 *  Each row's |payload| is a hunk reference into the hunk chunks containing blocks.
 */
struct ISnapshotWriter
    : public TRefCounted
{
    virtual TFuture<void> Open() = 0;

    //! Appends a batch of rows; blocks are expected in ascending index order.
    virtual TFuture<void> WriteBlocks(TRange<TSnapshotBlock> blocks) = 0;

    //! The distinct hunk chunks the rows written so far reference.
    virtual std::vector<NChunkClient::TChunkId> GetReferencedChunkIds() const = 0;

    //! Attaches the referenced chunks to the table's hunk chunk list and finishes the upload. The caller
    //! must have sealed them first.
    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISnapshotWriter)

////////////////////////////////////////////////////////////////////////////////

ISnapshotWriterPtr CreateSnapshotWriter(
    NApi::NNative::IClientPtr client,
    NChunkClient::TUserObject userObject,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
