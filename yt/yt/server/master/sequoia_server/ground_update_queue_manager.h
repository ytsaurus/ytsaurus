#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

struct IGroundUpdateQueueManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    template <typename TRecord>
    void EnqueueWrite(const TRecord& record);
    template <typename TRecordKey>
    void EnqueueDelete(const TRecordKey& recordKey);

    virtual void EnqueueRow(
        NSequoiaClient::EGroundUpdateQueue queue,
        NSequoiaClient::ESequoiaTable table,
        NTableClient::TUnversionedOwningRow row,
        EGroundUpdateAction action) = 0;

    virtual TFuture<void> Sync(
        NSequoiaClient::EGroundUpdateQueue queue) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGroundUpdateQueueManager)

////////////////////////////////////////////////////////////////////////////////

IGroundUpdateQueueManagerPtr CreateGroundUpdateQueueManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer

#define GROUND_UPDATE_QUEUE_MANAGER_INL_H_
#include "ground_update_queue_manager-inl.h"
#undef GROUND_UPDATE_QUEUE_MANAGER_INL_H_
