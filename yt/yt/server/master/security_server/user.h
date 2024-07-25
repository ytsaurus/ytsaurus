#pragma once

#include "public.h"
#include "subject.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/request_complexity_limits.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkloadStatistics
{
    std::atomic<i64> RequestCount = 0;

    //! Total request time in milliseconds.
    std::atomic<i64> RequestTime = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TUserRequestLimitsOptions
    : public NYTree::TYsonStruct
{
public:
    std::optional<int> Default;
    THashMap<NObjectServer::TCellTag, int> PerCell;

    void SetValue(NObjectServer::TCellTag cellTag, std::optional<int> value);
    std::optional<int> GetValue(NObjectServer::TCellTag cellTag) const;

    REGISTER_YSON_STRUCT(TUserRequestLimitsOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserRequestLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserQueueSizeLimitsOptions
    : public NYTree::TYsonStruct
{
public:
    int Default;
    THashMap<NObjectServer::TCellTag, int> PerCell;

    void SetValue(NObjectServer::TCellTag cellTag, int value);
    int GetValue(NObjectServer::TCellTag cellTag) const;

    REGISTER_YSON_STRUCT(TUserQueueSizeLimitsOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserQueueSizeLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserReadRequestComplexityLimitsOptions
    : public NYTree::TYsonStruct
{
public:
    std::optional<i64> NodeCount;
    std::optional<i64> ResultSize;

    NYTree::TReadRequestComplexityOverrides ToReadRequestComplexityOverrides() const noexcept;

    REGISTER_YSON_STRUCT(TUserReadRequestComplexityLimitsOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserReadRequestComplexityLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TUserRequestLimitsConfig
    : public NYTree::TYsonStruct
{
public:
    TUserRequestLimitsOptionsPtr ReadRequestRateLimits;
    TUserRequestLimitsOptionsPtr WriteRequestRateLimits;
    TUserQueueSizeLimitsOptionsPtr RequestQueueSizeLimits;
    TUserReadRequestComplexityLimitsOptionsPtr DefaultReadRequestComplexityLimits;
    TUserReadRequestComplexityLimitsOptionsPtr MaxReadRequestComplexityLimits;

    REGISTER_YSON_STRUCT(TUserRequestLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserRequestLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TSerializableUserRequestLimitsOptions
    : public NYTree::TYsonStruct
{
public:
    TUserRequestLimitsOptionsPtr ToLimitsOrThrow(const NCellMaster::IMulticellManagerPtr& multicellManager) const;

    static TSerializableUserRequestLimitsOptionsPtr CreateFrom(
        const TUserRequestLimitsOptionsPtr& options,
        const NCellMaster::IMulticellManagerPtr& multicellManager);

    REGISTER_YSON_STRUCT(TSerializableUserRequestLimitsOptions);

    static void Register(TRegistrar registrar);

private:
    std::optional<int> Default_;
    THashMap<TString, int> PerCell_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserRequestLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableUserQueueSizeLimitsOptions
    : public NYTree::TYsonStruct
{
public:
    static TSerializableUserQueueSizeLimitsOptionsPtr CreateFrom(
        const TUserQueueSizeLimitsOptionsPtr& options,
        const NCellMaster::IMulticellManagerPtr& multicellManager);

    TUserQueueSizeLimitsOptionsPtr ToLimitsOrThrow(const NCellMaster::IMulticellManagerPtr& multicellManager) const;

    REGISTER_YSON_STRUCT(TSerializableUserQueueSizeLimitsOptions);

    static void Register(TRegistrar registrar);

private:
    int Default_;
    THashMap<TString, int> PerCell_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserQueueSizeLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableUserReadRequestComplexityLimitsOptions
    : public NYTree::TYsonStruct
{
public:
    static TSerializableUserReadRequestComplexityLimitsOptionsPtr CreateFrom(
        const TUserReadRequestComplexityLimitsOptionsPtr& options);

    TUserReadRequestComplexityLimitsOptionsPtr ToLimitsOrThrow() const;

    REGISTER_YSON_STRUCT(TSerializableUserReadRequestComplexityLimitsOptions);

    static void Register(TRegistrar registrar);

private:
    std::optional<i64> NodeCount_;
    std::optional<i64> ResultSize_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserReadRequestComplexityLimitsOptions)

////////////////////////////////////////////////////////////////////////////////

class TSerializableUserRequestLimitsConfig
    : public NYTree::TYsonStruct
{
public:
    static TSerializableUserRequestLimitsConfigPtr CreateFrom(
        const TUserRequestLimitsConfigPtr& config,
        const NCellMaster::IMulticellManagerPtr& multicellManager);

    TUserRequestLimitsConfigPtr ToConfigOrThrow(const NCellMaster::IMulticellManagerPtr& multicellManager) const;

    REGISTER_YSON_STRUCT(TSerializableUserRequestLimitsConfig);

    static void Register(TRegistrar registrar);

private:
    TSerializableUserRequestLimitsOptionsPtr ReadRequestRateLimits_;
    TSerializableUserRequestLimitsOptionsPtr WriteRequestRateLimits_;
    TSerializableUserQueueSizeLimitsOptionsPtr RequestQueueSizeLimits_;
    TSerializableUserReadRequestComplexityLimitsOptionsPtr DefaultReadRequestComplexityLimits_;
    TSerializableUserReadRequestComplexityLimitsOptionsPtr MaxReadRequestComplexityLimits_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableUserRequestLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TSubject
{
public:
    // Limits, bans and removals.
    DEFINE_BYVAL_RW_PROPERTY(bool, Banned);
    DEFINE_BYVAL_RW_PROPERTY(TUserRequestLimitsConfigPtr, ObjectServiceRequestLimits);
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TThroughputThrottlerConfigPtr, ChunkServiceUserRequestWeightThrottlerConfig);
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TThroughputThrottlerConfigPtr, ChunkServiceUserRequestBytesThrottlerConfig);
    DEFINE_BYVAL_RW_PROPERTY(bool, PendingRemoval, false);
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowCreateSecondaryIndices, false);

    //! Hashed password used for authentication. If equals to |std::nullopt|,
    //! authentication via password is disabled.
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, HashedPassword);
    //! Salt used for password encryption.
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, PasswordSalt);
    //! Revision of the password that increases every time
    //! password changes.
    DEFINE_BYVAL_RO_PROPERTY(NHydra::TRevision, PasswordRevision);
    //! User tags used for ACE filtration.
    DEFINE_BYREF_RW_PROPERTY(TBooleanFormulaTags, Tags);

    //! Time of the last user's activity.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastSeenTime);

    int GetRequestQueueSize() const;
    void SetRequestQueueSize(int size);
    void ResetRequestQueueSize();

    //! Sets (encrypted) password for user. |std::nullopt| removes password.
    void SetHashedPassword(std::optional<TString> hashedPassword);
    //! Sets password salt for user. |std::nullopt| removes salt.
    void SetPasswordSalt(std::optional<TString> passwordSalt);

    using TStatistics = TEnumIndexedArray<EUserWorkloadType, TUserWorkloadStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TStatistics, Statistics);

public:
    using TSubject::TSubject;
    explicit TUser(TUserId id);

    void SetName(const TString& name) override;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const NConcurrency::IReconfigurableThroughputThrottlerPtr& GetRequestRateThrottler(EUserWorkloadType workloadType);
    void SetRequestRateThrottler(NConcurrency::IReconfigurableThroughputThrottlerPtr throttler, EUserWorkloadType workloadType);

    std::optional<int> GetRequestRateLimit(EUserWorkloadType workloadType, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag) const;
    void SetRequestRateLimit(std::optional<int> limit, EUserWorkloadType workloadType, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag);

    int GetRequestQueueSizeLimit(NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag) const;
    void SetRequestQueueSizeLimit(int limit, NObjectServer::TCellTag cellTag = NObjectClient::InvalidCellTag);

    void UpdateCounters(const TUserWorkload& workloadType);

    void AlertIfPendingRemoval(TStringBuf message) const;

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

    void InitializeCounters();
    void UpdatePasswordRevision();
};

DEFINE_MASTER_OBJECT_TYPE(TUser)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
