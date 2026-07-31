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
    //! The master cell hosting the device's chunks. A snapshot save table must be created co-located
    //! with this cell (see #CreateSnapshotTable) so it can reference the device's hunk chunks.
    virtual NObjectClient::TCellTag GetExternalCellTag() const = 0;

    //! Writes a snapshot of the device into the (already created and resolved) table |spec|.
    /*!
     *  The future is set once the rows have been written under |spec|'s transaction; the caller
     *  commits that transaction.
     */
    virtual TFuture<void> SaveSnapshot(const TSnapshotSaveSpec& spec) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalBlockDevice)

////////////////////////////////////////////////////////////////////////////////

IJournalBlockDevicePtr CreateJournalBlockDevice(
    NApi::NNative::IClientPtr client,
    std::string deviceId,
    TJournalBlockDeviceConfigPtr deviceConfig,
    TJournalBlockDeviceOptionsPtr storeOptions,
    NObjectClient::TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    std::optional<TSnapshotLoadSpec> snapshotReadSpec,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////
// Snapshot helpers

using TFetchSnapshotLoadTableSpecOptions = NTableClient::TFetchSingleTableReadSpecOptions;
using TCreateSnapshotTableOptions = NApi::TCreateNodeOptions;

struct TFetchSnapshotSaveTableSpecOptions
    : public NChunkClient::TGetUserObjectBasicAttributesOptions
{
    NTransactionClient::TTransactionId TransactionId;
};

//! Fetches a load spec for the existing snapshot table at |path|, to restore a device from it.
/*!
 *  Blocking; run it on a background invoker.
 */
TSnapshotLoadSpec FetchSnapshotLoadSpec(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    const TFetchSnapshotLoadTableSpecOptions& options = {});

//! Creates the sorted static table that holds a journal device snapshot at |path|.
//! If the snapshot references hunk chunks, |externalCellTag| must be provided so that
//! the table is pinned to the appropriate cell.
/*!
 *  Blocking; run it on a background invoker.
 */
void CreateSnapshotTable(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    std::optional<NObjectClient::TCellTag> externalCellTag,
    const TCreateSnapshotTableOptions& options = {});

//! Resolves the snapshot save table at |path| (created by #CreateSnapshotTable) into a save spec that
//! #SaveSnapshot writes into. The transaction the table was created under is carried in the spec.
/*!
 *  Blocking; run it on a background invoker.
 */
TSnapshotSaveSpec FetchSnapshotSaveSpec(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    const TFetchSnapshotSaveTableSpecOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
