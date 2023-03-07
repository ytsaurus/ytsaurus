#pragma once

#include "public.h"
#include "subject.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/object.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/misc/property.h>

#include <yt/core/concurrency/public.h>

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
    int Default;
    THashMap<NObjectServer::TCellTag, int> PerCell;

    TUserRequestLimitsOptions()
    {
        RegisterParameter("default", Default)
            .GreaterThan(0)
            .Default(100);
        RegisterParameter("per_cell", PerCell)
            .Optional();

        RegisterPostprocessor([&] () {
            for (const auto& [cellTag, value] : PerCell) {
                if (cellTag < NObjectClient::MinValidCellTag || cellTag > NObjectClient::MaxValidCellTag) {
                    THROW_ERROR_EXCEPTION("Invalid cell tag: %v",
                        cellTag);
                }

                if (value <= 0) {
                    THROW_ERROR_EXCEPTION("Value must be greater than zero (CellTag: %v, Value: %v)",
                        cellTag, value);
                }
            }
        });
    }

    void SetValue(NObjectServer::TCellTag cellTag, int value)
    {
        if (cellTag == NObjectClient::InvalidCellTag) {
            Default = value;
        } else {
            PerCell[cellTag] = value;
        }
    }

    int GetValue(NObjectServer::TCellTag cellTag) const
    {
        return PerCell.Value(cellTag, Default);
    }
};

DEFINE_REFCOUNTED_TYPE(TUserRequestLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserRequestLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    TUserRequestLimitsOptionsPtr ReadRequestRateLimits;
    TUserRequestLimitsOptionsPtr WriteRequestRateLimits;
    TUserRequestLimitsOptionsPtr RequestQueueSizeLimits;

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

    // Transient
    DEFINE_BYVAL_RW_PROPERTY(int, RequestQueueSize);
    DEFINE_BYVAL_RW_PROPERTY(bool, NeedsProfiling);

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

    int GetRequestRateLimit(EUserWorkloadType workloadType, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag) const;
    void SetRequestRateLimit(int limit, EUserWorkloadType workloadType, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag);

    int GetRequestQueueSizeLimit(NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag) const;
    void SetRequestQueueSizeLimit(int limit, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag);

private:
    NConcurrency::IReconfigurableThroughputThrottlerPtr ReadRequestRateThrottler_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr WriteRequestRateThrottler_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
