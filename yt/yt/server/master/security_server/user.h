#pragma once

#include "public.h"
#include "subject.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkloadStatistics
{
    i64 RequestCount = 0;
    TDuration RequestTime;
};

////////////////////////////////////////////////////////////////////////////////

class TUserRequestLimitsOptions
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> Default;
    THashMap<NObjectServer::TCellTag, int> PerCell;

    TUserRequestLimitsOptions();

    void SetValue(NObjectServer::TCellTag cellTag, std::optional<int> value);
    std::optional<int> GetValue(NObjectServer::TCellTag cellTag) const;
};

DEFINE_REFCOUNTED_TYPE(TUserRequestLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserQueueSizeLimitsOptions
    : public NYTree::TYsonSerializable
{
public:
    int Default;
    THashMap<NObjectServer::TCellTag, int> PerCell;

    TUserQueueSizeLimitsOptions();

    void SetValue(NObjectServer::TCellTag cellTag, int value);
    int GetValue(NObjectServer::TCellTag cellTag) const;
};

DEFINE_REFCOUNTED_TYPE(TUserQueueSizeLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserRequestLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    TUserRequestLimitsOptionsPtr ReadRequestRateLimits;
    TUserRequestLimitsOptionsPtr WriteRequestRateLimits;
    TUserQueueSizeLimitsOptionsPtr RequestQueueSizeLimits;

    TUserRequestLimitsConfig()
    {
        RegisterParameter("read_request_rate", ReadRequestRateLimits)
            .DefaultNew();
        RegisterParameter("write_request_rate", WriteRequestRateLimits)
            .DefaultNew();
        RegisterParameter("request_queue_size", RequestQueueSizeLimits)
            .DefaultNew();

        RegisterPostprocessor([&] () {
            if (!ReadRequestRateLimits) {
                THROW_ERROR_EXCEPTION("\"read_request_rate\" must be set");
            }
            if (!WriteRequestRateLimits) {
                THROW_ERROR_EXCEPTION("\"write_request_rate\" must be set");
            }
            if (!RequestQueueSizeLimits) {
                THROW_ERROR_EXCEPTION("\"request_queue_size\" must be set");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TUserRequestLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TSubject
{
public:
    // Limits and bans.
    DEFINE_BYVAL_RW_PROPERTY(bool, Banned);
    DEFINE_BYVAL_RW_PROPERTY(TUserRequestLimitsConfigPtr, RequestLimits);

    int GetRequestQueueSize() const;
    void SetRequestQueueSize(int size);

    using TStatistics = TEnumIndexedVector<EUserWorkloadType, TUserWorkloadStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TStatistics, Statistics);

public:
    explicit TUser(TUserId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const NConcurrency::IReconfigurableThroughputThrottlerPtr& GetRequestRateThrottler(EUserWorkloadType workloadType);
    void SetRequestRateThrottler(NConcurrency::IReconfigurableThroughputThrottlerPtr throttler, EUserWorkloadType workloadType);

    std::optional<int> GetRequestRateLimit(EUserWorkloadType workloadType, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag) const;
    void SetRequestRateLimit(std::optional<int> limit, EUserWorkloadType workloadType, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag);

    int GetRequestQueueSizeLimit(NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag) const;
    void SetRequestQueueSizeLimit(int limit, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag);

    void UpdateCounters(const TUserWorkload& workloadType);

protected:
    // Transient
    int RequestQueueSize_ = 0;

private:
    NConcurrency::IReconfigurableThroughputThrottlerPtr ReadRequestRateThrottler_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr WriteRequestRateThrottler_;

    NProfiling::TTimeCounter ReadTimeCounter_;
    NProfiling::TTimeCounter WriteTimeCounter_;
    NProfiling::TCounter RequestCounter_;
    NProfiling::TCounter ReadRequestCounter_;
    NProfiling::TCounter WriteRequestCounter_;
    NProfiling::TSummary RequestQueueSizeSummary_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
