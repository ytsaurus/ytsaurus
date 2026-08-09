#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/logging/public.h>

#include <optional>
#include <variant>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! A journal-backed block device that can additionally persist a point-in-time snapshot of its
//! contents to a Cypress table.
/*!
 *  Recover it from an IBlockDevice with DynamicPointerCast.
 */
struct IJournalBlockDevice
    : public virtual IBlockDevice
{
    //! The options the device actually runs with.
    virtual TJournalBlockDeviceOptionsPtr GetOptions() const = 0;

    //! The master cell hosting the device's chunks. A snapshot save table must be created co-located
    //! with this cell (see #CreateSnapshotTable) so it can reference the device's hunk chunks.
    virtual NObjectClient::TCellTag GetExternalCellTag() const = 0;

    struct TSnapshotSaveResult
    {
        //! Live blocks written as rows into the snapshot table.
        i64 BlockCount = 0;
        //! Distinct hunk chunks those blocks reference.
        int ChunkCount = 0;
    };

    //! Writes a snapshot of the device into the (already created and resolved) table |spec|.
    /*!
     *  The future is set once the rows have been written under |spec|'s transaction; the caller
     *  commits that transaction.
     */
    virtual TFuture<TSnapshotSaveResult> SaveSnapshot(const TSnapshotSaveSpec& spec) = 0;

    //! Flushes every block written as of this call into the store.
    /*!
     *  Until flushed a block is a dirty pool resident, not a journal record. Fails if the flusher
     *  has failed or has been stopped by #Finalize.
     */
    virtual TFuture<void> FlushBlocks() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalBlockDevice)

////////////////////////////////////////////////////////////////////////////////

struct TFreshDeviceCreationDescriptor
{
    TJournalBlockDeviceOptionsPtr Options;
};

struct TRestoredDeviceCreationDescriptor
{
    NYPath::TYPath SnapshotPath;
};

using TDeviceCreationDescriptor = std::variant<
    TFreshDeviceCreationDescriptor,
    TRestoredDeviceCreationDescriptor
>;

TFuture<IJournalBlockDevicePtr> CreateJournalBlockDevice(
    NApi::NNative::IClientPtr client,
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    TDeviceCreationDescriptor creationDescriptor,
    NObjectClient::TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////
// Snapshot helpers

using TCreateSnapshotTableOptions = NApi::TCreateNodeOptions;

struct TFetchSnapshotSaveTableSpecOptions
    : public NChunkClient::TGetUserObjectBasicAttributesOptions
{
    NTransactionClient::TTransactionId TransactionId;
};

//! Creates the sorted static table that holds a journal device snapshot at |path|.
//! If the snapshot references hunk chunks,
//! |externalCellTag| must be provided so that the table is pinned to the appropriate cell.
/*!
 *  The work runs on the client's connection invoker.
 */
TFuture<void> CreateSnapshotTable(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    std::optional<NObjectClient::TCellTag> externalCellTag,
    const TJournalBlockDeviceOptionsPtr& deviceOptions,
    const TCreateSnapshotTableOptions& options = {});

//! Resolves the snapshot save table at |path| (created by #CreateSnapshotTable) into a save spec that
//! #SaveSnapshot writes into.
/*!
 *  The work runs on the client's connection invoker.
 */
TFuture<TSnapshotSaveSpec> FetchSnapshotSaveSpec(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    const TFetchSnapshotSaveTableSpecOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
