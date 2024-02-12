#include "user.h"
#include "helpers.h"

#include "yt/yt/server/master/security_server/private.h"

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/request_complexity_limiter.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static void ValidateCellTags(const auto& perCell)
{
    for (const auto& [cellTag, value] : perCell) {
        if (cellTag < MinValidCellTag || cellTag > MaxValidCellTag) {
            THROW_ERROR_EXCEPTION("Invalid cell tag %v",
                cellTag);
        }

        if (value <= 0) {
            THROW_ERROR_EXCEPTION("Invalid limit for cell %v: value %v must be greater than zero",
                cellTag,
                value);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TUserRequestLimitsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("default", &TThis::Default)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("per_cell", &TThis::PerCell)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        ValidateCellTags(config->PerCell);
    });
}

void TUserRequestLimitsOptions::SetValue(TCellTag cellTag, std::optional<int> value)
{
    if (cellTag == InvalidCellTag) {
        Default = value;
    } else {
        YT_VERIFY(value);
        PerCell[cellTag] = *value;
    }
}

std::optional<int> TUserRequestLimitsOptions::GetValue(TCellTag cellTag) const
{
    if (auto it = PerCell.find(cellTag)) {
        return it->second;
    }
    return Default;
}

////////////////////////////////////////////////////////////////////////////////

void TUserQueueSizeLimitsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("default", &TThis::Default)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("per_cell", &TThis::PerCell)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        ValidateCellTags(config->PerCell);
    });
}

void TUserQueueSizeLimitsOptions::SetValue(TCellTag cellTag, int value)
{
    if (cellTag == InvalidCellTag) {
        Default = value;
    } else {
        PerCell[cellTag] = value;
    }
}

int TUserQueueSizeLimitsOptions::GetValue(TCellTag cellTag) const
{
    return PerCell.Value(cellTag, Default);
}

////////////////////////////////////////////////////////////////////////////////

void TUserReadRequestComplexityLimitsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("node_count", &TThis::NodeCount)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("result_size", &TThis::ResultSize)
        .Optional()
        .GreaterThanOrEqual(0);
}

TReadRequestComplexityOverrides
TUserReadRequestComplexityLimitsOptions::ToReadRequestComplexityOverrides() const noexcept
{
    return {
        .NodeCount = NodeCount,
        .ResultSize = ResultSize,
    };
}

////////////////////////////////////////////////////////////////////////////////

void TUserRequestLimitsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_request_rate", &TThis::ReadRequestRateLimits)
        .DefaultNew();
    registrar.Parameter("write_request_rate", &TThis::WriteRequestRateLimits)
        .DefaultNew();
    registrar.Parameter("request_queue_size", &TThis::RequestQueueSizeLimits)
        .DefaultNew();
    registrar.Parameter("default_read_request_complexity", &TThis::DefaultReadRequestComplexityLimits)
        .DefaultNew();
    registrar.Parameter("max_read_request_complexity", &TThis::MaxReadRequestComplexityLimits)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (!config->ReadRequestRateLimits) {
            THROW_ERROR_EXCEPTION("\"read_request_rate\" must be set");
        }
        if (!config->WriteRequestRateLimits) {
            THROW_ERROR_EXCEPTION("\"write_request_rate\" must be set");
        }
        if (!config->RequestQueueSizeLimits) {
            THROW_ERROR_EXCEPTION("\"request_queue_size\" must be set");
        }
        if (!config->DefaultReadRequestComplexityLimits) {
            THROW_ERROR_EXCEPTION("\"default_read_request_complexity\" must be set");
        }
        if (!config->MaxReadRequestComplexityLimits) {
            THROW_ERROR_EXCEPTION("\"max_read_request_complexity\" must be set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSerializableUserRequestLimitsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("default", &TThis::Default_)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("per_cell", &TThis::PerCell_)
        .Optional();
}

TSerializableUserRequestLimitsOptionsPtr TSerializableUserRequestLimitsOptions::CreateFrom(
    const TUserRequestLimitsOptionsPtr& options,
    const IMulticellManagerPtr& multicellManager)
{
    auto result = New<TSerializableUserRequestLimitsOptions>();

    result->Default_ = options->Default;
    result->PerCell_ = CellTagMapToCellNameMap(options->PerCell, multicellManager);

    return result;
}

TUserRequestLimitsOptionsPtr TSerializableUserRequestLimitsOptions::ToLimitsOrThrow(
    const IMulticellManagerPtr& multicellManager) const
{
    auto result = New<TUserRequestLimitsOptions>();
    result->Default = Default_;
    result->PerCell = CellNameMapToCellTagMapOrThrow(PerCell_, multicellManager);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TSerializableUserQueueSizeLimitsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("default", &TThis::Default_)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("per_cell", &TThis::PerCell_)
        .Optional();
}

TSerializableUserQueueSizeLimitsOptionsPtr TSerializableUserQueueSizeLimitsOptions::CreateFrom(
    const TUserQueueSizeLimitsOptionsPtr& options,
    const IMulticellManagerPtr& multicellManager)
{
    auto result = New<TSerializableUserQueueSizeLimitsOptions>();

    result->Default_ = options->Default;
    result->PerCell_ = CellTagMapToCellNameMap(options->PerCell, multicellManager);

    return result;
}

TUserQueueSizeLimitsOptionsPtr TSerializableUserQueueSizeLimitsOptions::ToLimitsOrThrow(
    const IMulticellManagerPtr& multicellManager) const
{
    auto result = New<TUserQueueSizeLimitsOptions>();
    result->Default = Default_;
    result->PerCell = CellNameMapToCellTagMapOrThrow(PerCell_, multicellManager);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TSerializableUserReadRequestComplexityLimitsOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("node_count", &TThis::NodeCount_)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("result_size", &TThis::ResultSize_)
        .Optional()
        .GreaterThanOrEqual(0);
}

TSerializableUserReadRequestComplexityLimitsOptionsPtr TSerializableUserReadRequestComplexityLimitsOptions::CreateFrom(
    const TUserReadRequestComplexityLimitsOptionsPtr& options)
{
    auto result = New<TSerializableUserReadRequestComplexityLimitsOptions>();

    result->NodeCount_ = options->NodeCount;
    result->ResultSize_ = options->ResultSize;

    return result;
}

TUserReadRequestComplexityLimitsOptionsPtr TSerializableUserReadRequestComplexityLimitsOptions::ToLimitsOrThrow() const
{
    auto result = New<TUserReadRequestComplexityLimitsOptions>();
    result->NodeCount = NodeCount_;
    result->ResultSize = ResultSize_;
    return result;
}

////////////////////////////////////////////////////////////////////////////////


void TSerializableUserRequestLimitsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_request_rate", &TThis::ReadRequestRateLimits_)
        .DefaultNew();
    registrar.Parameter("write_request_rate", &TThis::WriteRequestRateLimits_)
        .DefaultNew();
    registrar.Parameter("request_queue_size", &TThis::RequestQueueSizeLimits_)
        .DefaultNew();
    registrar.Parameter("default_read_request_complexity", &TThis::DefaultReadRequestComplexityLimits_)
        .DefaultNew();
    registrar.Parameter("max_read_request_complexity", &TThis::MaxReadRequestComplexityLimits_)
        .DefaultNew();
}

TSerializableUserRequestLimitsConfigPtr TSerializableUserRequestLimitsConfig::CreateFrom(
    const TUserRequestLimitsConfigPtr& config,
    const IMulticellManagerPtr& multicellManager)
{
    auto result = New<TSerializableUserRequestLimitsConfig>();

    result->ReadRequestRateLimits_ = TSerializableUserRequestLimitsOptions::CreateFrom(config->ReadRequestRateLimits, multicellManager);
    result->WriteRequestRateLimits_ = TSerializableUserRequestLimitsOptions::CreateFrom(config->WriteRequestRateLimits, multicellManager);
    result->RequestQueueSizeLimits_ = TSerializableUserQueueSizeLimitsOptions::CreateFrom(config->RequestQueueSizeLimits, multicellManager);
    result->DefaultReadRequestComplexityLimits_ = TSerializableUserReadRequestComplexityLimitsOptions::CreateFrom(config->DefaultReadRequestComplexityLimits);
    result->MaxReadRequestComplexityLimits_ = TSerializableUserReadRequestComplexityLimitsOptions::CreateFrom(config->MaxReadRequestComplexityLimits);

    return result;
}

TUserRequestLimitsConfigPtr TSerializableUserRequestLimitsConfig::ToConfigOrThrow(
    const IMulticellManagerPtr& multicellManager) const
{
    auto result = New<TUserRequestLimitsConfig>();
    result->ReadRequestRateLimits = ReadRequestRateLimits_->ToLimitsOrThrow(multicellManager);
    result->WriteRequestRateLimits = WriteRequestRateLimits_->ToLimitsOrThrow(multicellManager);
    result->RequestQueueSizeLimits = RequestQueueSizeLimits_->ToLimitsOrThrow(multicellManager);
    result->DefaultReadRequestComplexityLimits = DefaultReadRequestComplexityLimits_->ToLimitsOrThrow();
    result->MaxReadRequestComplexityLimits = MaxReadRequestComplexityLimits_->ToLimitsOrThrow();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(TUserId id)
    : TSubject(id)
    , ObjectServiceRequestLimits_(New<TUserRequestLimitsConfig>())
{ }

void TUser::SetName(const TString& name)
{
    TSubject::SetName(name);
    InitializeCounters();
}

TString TUser::GetLowercaseObjectName() const
{
    return Format("user %Qv", Name_);
}

TString TUser::GetCapitalizedObjectName() const
{
    return Format("User %Qv", Name_);
}

TString TUser::GetObjectPath() const
{
    return Format("//sys/users/%v", GetName());
}

void TUser::Save(TSaveContext& context) const
{
    TSubject::Save(context);

    using NYT::Save;
    Save(context, Banned_);
    Save(context, HashedPassword_);
    Save(context, PasswordSalt_);
    Save(context, PasswordRevision_);
    Save(context, *ObjectServiceRequestLimits_);
    Save(context, Tags_);
    TNullableIntrusivePtrSerializer<>::Save(context, ChunkServiceUserRequestWeightThrottlerConfig_);
    TNullableIntrusivePtrSerializer<>::Save(context, ChunkServiceUserRequestBytesThrottlerConfig_);
}

void TUser::Load(TLoadContext& context)
{
    TSubject::Load(context);

    using NYT::Load;
    Load(context, Banned_);
    Load(context, HashedPassword_);
    Load(context, PasswordSalt_);
    Load(context, PasswordRevision_);
    Load(context, *ObjectServiceRequestLimits_);

    // COMPAT(vovamelnikov)
    if (context.GetVersion() >= EMasterReign::AttributeBasedAccessControl) {
        Load(context, Tags_);
    }

    TNullableIntrusivePtrSerializer<>::Load(context, ChunkServiceUserRequestWeightThrottlerConfig_);
    TNullableIntrusivePtrSerializer<>::Load(context, ChunkServiceUserRequestBytesThrottlerConfig_);

    InitializeCounters();
}

void TUser::InitializeCounters()
{
    auto profiler = SecurityProfiler
        .WithSparse()
        .WithTag("user", Name_);

    ReadTimeCounter_ = profiler.TimeCounter("/user_read_time");
    WriteTimeCounter_ = profiler.TimeCounter("/user_write_time");
    ReadRequestCounter_ = profiler.Counter("/user_read_request_count");
    WriteRequestCounter_ = profiler.Counter("/user_write_request_count");
    RequestCounter_ = profiler.Counter("/user_request_count");
    RequestQueueSizeSummary_ = profiler.Summary("/user_request_queue_size");
}

int TUser::GetRequestQueueSize() const
{
    return RequestQueueSize_;
}

void TUser::SetRequestQueueSize(int size)
{
    RequestQueueSize_ = size;
    RequestQueueSizeSummary_.Record(size);
}

void TUser::ResetRequestQueueSize()
{
    RequestQueueSize_ = 0;
}

void TUser::SetHashedPassword(std::optional<TString> hashedPassword)
{
    HashedPassword_ = std::move(hashedPassword);

    UpdatePasswordRevision();
}

void TUser::SetPasswordSalt(std::optional<TString> passwordSalt)
{
    PasswordSalt_ = std::move(passwordSalt);

    UpdatePasswordRevision();
}

void TUser::UpdateCounters(const TUserWorkload& workload)
{
    RequestCounter_.Increment(workload.RequestCount);
    switch (workload.Type) {
        case EUserWorkloadType::Read:
            ReadRequestCounter_.Increment(workload.RequestCount);
            ReadTimeCounter_.Add(workload.RequestTime);
            break;
        case EUserWorkloadType::Write:
            WriteRequestCounter_.Increment(workload.RequestCount);
            WriteTimeCounter_.Add(workload.RequestTime);
            break;
        default:
            YT_ABORT();
    }
}

const IReconfigurableThroughputThrottlerPtr& TUser::GetRequestRateThrottler(EUserWorkloadType workloadType)
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (workloadType) {
        case EUserWorkloadType::Read:
            return ReadRequestRateThrottler_;
        case EUserWorkloadType::Write:
            return WriteRequestRateThrottler_;
        default:
            YT_ABORT();
    }
}

void TUser::SetRequestRateThrottler(
    IReconfigurableThroughputThrottlerPtr throttler,
    EUserWorkloadType workloadType)
{
    switch (workloadType) {
        case EUserWorkloadType::Read:
            ReadRequestRateThrottler_ = std::move(throttler);
            break;
        case EUserWorkloadType::Write:
            WriteRequestRateThrottler_ = std::move(throttler);
            break;
        default:
            YT_ABORT();
    }
}

std::optional<int> TUser::GetRequestRateLimit(EUserWorkloadType type, NObjectServer::TCellTag cellTag) const
{
    switch (type) {
        case EUserWorkloadType::Read:
            return ObjectServiceRequestLimits_->ReadRequestRateLimits->GetValue(cellTag);
        case EUserWorkloadType::Write:
            return ObjectServiceRequestLimits_->WriteRequestRateLimits->GetValue(cellTag);
        default:
            YT_ABORT();
    }
}

void TUser::SetRequestRateLimit(std::optional<int> limit, EUserWorkloadType type, NObjectServer::TCellTag cellTag)
{
    switch (type) {
        case EUserWorkloadType::Read:
            ObjectServiceRequestLimits_->ReadRequestRateLimits->SetValue(cellTag, limit);
            break;
        case EUserWorkloadType::Write:
            ObjectServiceRequestLimits_->WriteRequestRateLimits->SetValue(cellTag, limit);
            break;
        default:
            YT_ABORT();
    }
}

int TUser::GetRequestQueueSizeLimit(NObjectServer::TCellTag cellTag) const
{
    return ObjectServiceRequestLimits_->RequestQueueSizeLimits->GetValue(cellTag);
}

void TUser::SetRequestQueueSizeLimit(int limit, NObjectServer::TCellTag cellTag)
{
    ObjectServiceRequestLimits_->RequestQueueSizeLimits->SetValue(cellTag, limit);
}

void TUser::UpdatePasswordRevision()
{
    auto* hydraContext = NHydra::GetCurrentHydraContext();
    PasswordRevision_ = hydraContext->GetVersion().ToRevision();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

