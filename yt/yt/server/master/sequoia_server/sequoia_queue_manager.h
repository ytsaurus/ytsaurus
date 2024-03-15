#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaRecordAction,
    ((Write)              (0))
    ((Delete)             (1))
);

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaQueueManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    template <typename TRecord>
    void EnqueueWrite(const TRecord& record);
    template <typename TRecordKey>
    void EnqueueDelete(const TRecordKey& recordKey);

protected:
    virtual void EnqueueRow(
        NSequoiaClient::ESequoiaTable table,
        NTableClient::TUnversionedOwningRow row,
        ESequoiaRecordAction action) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaQueueManager)

////////////////////////////////////////////////////////////////////////////////

ISequoiaQueueManagerPtr CreateSequoiaQueueManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer

#define SEQUOIA_QUEUE_MANAGER_INL_H_
#include "sequoia_queue_manager-inl.h"
#undef SEQUOIA_QUEUE_MANAGER_INL_H_
