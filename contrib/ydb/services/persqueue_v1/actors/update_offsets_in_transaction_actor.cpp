#include "update_offsets_in_transaction_actor.h"
#include <contrib/ydb/core/base/feature_flags.h>


namespace NKikimr::NGRpcService {

TUpdateOffsetsInTransactionActor::TUpdateOffsetsInTransactionActor(IRequestOpCtx* request)
    : TBase{request}
{
}

void TUpdateOffsetsInTransactionActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    Become(&TUpdateOffsetsInTransactionActor::StateWork);
    Proceed(ctx);
}

void TUpdateOffsetsInTransactionActor::Proceed(const NActors::TActorContext& ctx)
{
    if (!AppData(ctx)->FeatureFlags.GetEnableTopicServiceTx()) {
        return Reply(Ydb::StatusIds::UNSUPPORTED,
                     "Disabled transaction support for TopicService.",
                     NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                     ctx);
    }

    const auto req = GetProtoRequest();

    if (!req->has_tx()) {
        return Reply(Ydb::StatusIds::BAD_REQUEST,
                     "Empty tx.",
                     NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                     ctx);
    }

    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    SetAuthToken(ev, *Request_);
    SetDatabase(ev, *Request_);

    if (CheckSession(req->tx().session(), Request_.get())) {
        ev->Record.MutableRequest()->SetSessionId(req->tx().session());
    } else {
        return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
    }

    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);

    if (const auto traceId = Request_->GetTraceId(); traceId) {
        ev->Record.SetTraceId(traceId.GetRef());
    }

    if (const auto requestType = Request_->GetRequestType(); requestType) {
        ev->Record.SetRequestType(requestType.GetRef());
    }

    ev->Record.MutableRequest()->SetCancelAfterMs(GetCancelAfter().MilliSeconds());
    ev->Record.MutableRequest()->SetTimeoutMs(GetOperationTimeout().MilliSeconds());

    ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(req->tx().id());

    ev->Record.MutableRequest()->MutableTopicOperations()->SetConsumer(req->consumer());

    for (const auto& topic : req->topics()) {
        auto* newTopic = ev->Record.MutableRequest()->MutableTopicOperations()->AddTopics();
        newTopic->set_path(topic.path());

        for (const auto& partition : topic.partitions()) {
            auto* newPartition = newTopic->add_partitions();
            newPartition->set_partition_id(partition.partition_id());

            for (const auto& offsetsRange : partition.partition_offsets()) {
                auto* newOffsetsRange = newPartition->add_partition_offsets();
                newOffsetsRange->set_start(offsetsRange.start());
                newOffsetsRange->set_end(offsetsRange.end());
            }
        }
    }

    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
}

void TUpdateOffsetsInTransactionActor::Handle(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev,
                                              const NActors::TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    SetCost(record.GetConsumedRu());
    AddServerHintsIfAny(record);

    if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
        const auto& kqpResponse = record.GetResponse();
        const auto& issueMessage = kqpResponse.GetQueryIssues();
        auto queryResult = TEvUpdateOffsetsInTransactionRequest::AllocateResult<Ydb::Topic::UpdateOffsetsInTransactionResult>(Request_);

        //
        // TODO: сохранить результат
        //

        //
        // TODO: статистика
        //

        //
        // TODO: tx_meta
        //

        ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, *queryResult, ctx);
    } else {
        OnGenericQueryResponseError(record, ctx);
    }
}

}
