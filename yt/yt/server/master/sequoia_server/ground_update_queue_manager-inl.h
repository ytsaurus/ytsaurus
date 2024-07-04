#ifndef GROUND_UPDATE_QUEUE_MANAGER_INL_H_
#error "Direct inclusion of this file is not allowed, include ground_update_queue_manager.h"
// For the sake of sane code completion.
#include "ground_update_queue_manager.h"
#endif

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

template <typename TRecord>
void IGroundUpdateQueueManager::EnqueueWrite(const TRecord& record)
{
    YT_VERIFY(NHydra::HasMutationContext());

    EnqueueRow(
        TRecord::Queue,
        TRecord::Table,
        NTableClient::TUnversionedOwningRow(NTableClient::FromRecord(
            record,
            New<NTableClient::TRowBuffer>())),
        EGroundUpdateAction::Write);
}

template <typename TRecordKey>
void IGroundUpdateQueueManager::EnqueueDelete(const TRecordKey& recordKey)
{
    YT_VERIFY(NHydra::HasMutationContext());

    EnqueueRow(
        TRecordKey::Queue,
        TRecordKey::Table,
        NTableClient::TUnversionedOwningRow(NTableClient::FromRecordKey(
            recordKey,
            New<NTableClient::TRowBuffer>())),
        EGroundUpdateAction::Delete);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
