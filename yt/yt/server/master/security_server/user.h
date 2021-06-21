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

    TUserRequestLimitsConfig();
};

DEFINE_REFCOUNTED_TYPE(TUserRequestLimitsConfig)


////////////////////////////////////////////////////////////////////////////////

class TSerializableUserRequestLimitsOptions
    : public NYTree::TYsonSerializable
{
public:
    TSerializableUserRequestLimitsOptions();
    TSerializableUserRequestLimitsOptions(
        const TUserRequestLimitsOptionsPtr& options,
        const NCellMaster::TMulticellManagerPtr& multicellManager);

    TUserRequestLimitsOptionsPtr ToLimitsOrThrow(const NCellMaster::TMulticellManagerPtr& multicellManager) const;

private:
    std::optional<int> Default_;
    THashMap<TString, int> PerCell_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserRequestLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableUserQueueSizeLimitsOptions
    : public NYTree::TYsonSerializable
{
public:
    TSerializableUserQueueSizeLimitsOptions();
    TSerializableUserQueueSizeLimitsOptions(
        const TUserQueueSizeLimitsOptionsPtr& options,
        const NCellMaster::TMulticellManagerPtr& multicellManager);

    TUserQueueSizeLimitsOptionsPtr ToLimitsOrThrow(const NCellMaster::TMulticellManagerPtr& multicellManager) const;

private:
    int Default_;
    THashMap<TString, int> PerCell_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserQueueSizeLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableUserRequestLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    TSerializableUserRequestLimitsConfig();
    TSerializableUserRequestLimitsConfig(
        const TUserRequestLimitsConfigPtr& config, 
        const NCellMaster::TMulticellManagerPtr& multicellManager);

    TUserRequestLimitsConfigPtr ToConfigOrThrow(const NCellMaster::TMulticellManagerPtr& multicellManager) const;

private:
    TSerializableUserRequestLimitsOptionsPtr ReadRequestRateLimits_;
    TSerializableUserRequestLimitsOptionsPtr WriteRequestRateLimits_;
    TSerializableUserQueueSizeLimitsOptionsPtr RequestQueueSizeLimits_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserRequestLimitsConfig)

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
    void ResetRequestQueueSize();

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
