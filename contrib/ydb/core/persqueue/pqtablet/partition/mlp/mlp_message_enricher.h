#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <contrib/ydb/core/base/tablet_pipecache.h>
#include <contrib/ydb/core/persqueue/common/actor.h>
#include <contrib/ydb/core/protos/pqconfig.pb.h>
#include <contrib/ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TMessageEnricherActor : public TBaseActor<TMessageEnricherActor>
                            , public TConstantLogPrefix {

public:
    TMessageEnricherActor(ui64 tabletId,
                          const ui32 partitionId,
                          const TString& consumerName,
                          std::deque<TReadResult>&& replies);

    void Bootstrap();
    void PassAway() override;

private:
    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    STFUNC(StateWork);

    void ProcessQueue();
    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

private:
    const ui64 TabletId;
    const ui32 PartitionId;
    const TString ConsumerName;
    std::deque<TReadResult> Queue;
    std::unique_ptr<TEvPQ::TEvMLPReadResponse> PendingResponse;

    bool FirstRequest = true;
};

} // namespace NKikimr::NPQ::NMLP
