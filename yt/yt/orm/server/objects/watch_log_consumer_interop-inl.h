#ifndef WATCH_LOG_CONSUMER_INTEROP_INL_H_
#error "Direct inclusion of this file is not allowed, include watch_log_consumer_interop.h"
// For the sake of sane code completion.
#include "watch_log_consumer_interop.h"
#endif

#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <
    class TConsumersTable,
    class TProtoWatchLogConsumerStatus,
    class TProtoWatchLogConsumerSpec>
class TWatchLogConsumerInterop
    : public IWatchLogConsumerInterop
{
public:
    TWatchLogConsumerInterop(const TConsumersTable* table)
        : Table_(table)
    { }

    TString GetQueryString(const NMaster::TYTConnectorPtr& ytConnector) override
    {
        return Format(
            "[%v], [%v], [%v] from [%v] where is_null([%v])",
            Table_->Fields.MetaId.Name,
            Table_->Fields.Spec.Name,
            Table_->Fields.Status.Name,
            ytConnector->GetTablePath(Table_),
            Table_->Fields.MetaRemovalTime.Name);
    }

    TSnapshotWatchLogConsumer Parse(NTableClient::TUnversionedRow row) override
    {
        TObjectId id;
        TProtoWatchLogConsumerSpec spec;
        TProtoWatchLogConsumerStatus status;

        NTableClient::FromUnversionedRow(
            row,
            &id,
            &spec,
            &status);

        TSnapshotWatchLogConsumer consumer{
            .Id = std::move(id),
            .ObjectType = spec.object_type(),
        };
        if (status.has_timestamp()) {
            consumer.Timestamp = status.timestamp();
        }
        if (status.has_continuation_token()) {
            consumer.ContinuationToken = status.continuation_token();
        }
        return consumer;
    }

private:
    const TConsumersTable* Table_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <
    class TConsumersTable,
    class TProtoWatchLogConsumerStatus,
    class TProtoWatchLogConsumerSpec>
IWatchLogConsumerInteropPtr CreateWatchLogConsumerInterop(const TConsumersTable* table)
{
    return New<NDetail::TWatchLogConsumerInterop<
        TConsumersTable,
        TProtoWatchLogConsumerStatus,
        TProtoWatchLogConsumerSpec>>(table);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
