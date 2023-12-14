#pragma once

#include <optional>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt_proto/yt/client/bundle_controller/proto/bundle_controller_service.pb.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCpuLimits)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TInstanceResources)
DECLARE_REFCOUNTED_STRUCT(THulkInstanceResources)

////////////////////////////////////////////////////////////////////////////////

struct TCpuLimits
    : public NYTree::TYsonStruct
{
    int LookupThreadPoolSize;
    int QueryThreadPoolSize;
    int WriteThreadPoolSize;

    REGISTER_YSON_STRUCT(TCpuLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCpuLimits)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryLimits
    : public NYTree::TYsonStruct
{
    std::optional<i64> CompressedBlockCache;
    std::optional<i64> KeyFilterBlockCache;
    std::optional<i64> LookupRowCache;
    std::optional<i64> TabletDynamic;
    std::optional<i64> TabletStatic;
    std::optional<i64> UncompressedBlockCache;
    std::optional<i64> VersionedChunkMeta;

    REGISTER_YSON_STRUCT(TMemoryLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryLimits)

////////////////////////////////////////////////////////////////////////////////

struct TInstanceResources
    : public NYTree::TYsonStruct
{
    i64 Memory;
    std::optional<i64> Net;

    TString Type;
    int Vcpu;

    TInstanceResources& operator=(const THulkInstanceResources& resources);

    bool operator==(const TInstanceResources& resources) const;

    void Clear();

    REGISTER_YSON_STRUCT(TInstanceResources);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInstanceResources)

////////////////////////////////////////////////////////////////////////////////

struct THulkInstanceResources
    : public NYTree::TYsonStruct
{
    int Vcpu;
    i64 MemoryMb;
    std::optional<int> NetworkBandwidth;

    THulkInstanceResources& operator=(const TInstanceResources& resources);

    REGISTER_YSON_STRUCT(THulkInstanceResources);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THulkInstanceResources)

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NBundleController::NProto::TCpuLimits* protoCpuLimits, const NCellBalancer::TCpuLimitsPtr cpuLimits);
void FromProto(NCellBalancer::TCpuLimitsPtr cpuLimits, const NBundleController::NProto::TCpuLimits* protoCpuLimits);

void ToProto(NBundleController::NProto::TMemoryLimits* protoMemoryLimits, const NCellBalancer::TMemoryLimitsPtr memoryLimits);
void FromProto(NCellBalancer::TMemoryLimitsPtr memoryLimits, const NBundleController::NProto::TMemoryLimits* protoMemoryLimits);

void ToProto(NBundleController::NProto::TInstanceResources* protoInstanceResources, const NCellBalancer::TInstanceResourcesPtr instanceResources);
void FromProto(NCellBalancer::TInstanceResourcesPtr instanceResources, const NBundleController::NProto::TInstanceResources* protoInstanceResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NYT::NCellBalancer
