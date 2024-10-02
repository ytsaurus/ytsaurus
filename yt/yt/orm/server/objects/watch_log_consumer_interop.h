#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <optional>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotWatchLogConsumer
{
    TObjectId Id;
    TString ObjectType;
    std::optional<TTimestamp> Timestamp;
    std::optional<TString> ContinuationToken;
};

////////////////////////////////////////////////////////////////////////////////

struct IWatchLogConsumerInterop
    : public TRefCounted
{
    virtual TString GetQueryString(const NMaster::TYTConnectorPtr& ytConnector) = 0;

    virtual TSnapshotWatchLogConsumer Parse(NTableClient::TUnversionedRow row) = 0;
};

DEFINE_REFCOUNTED_TYPE(IWatchLogConsumerInterop)

////////////////////////////////////////////////////////////////////////////////

template <
    class TConsumersTable,
    class TProtoWatchLogConsumerStatus,
    class TProtoWatchLogConsumerSpec>
IWatchLogConsumerInteropPtr CreateWatchLogConsumerInterop(const TConsumersTable* table);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define WATCH_LOG_CONSUMER_INTEROP_INL_H_
#include "watch_log_consumer_interop-inl.h"
#undef WATCH_LOG_CONSUMER_INTEROP_INL_H_
