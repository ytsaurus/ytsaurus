#pragma once

#include <yt/yql/providers/dq/config/config.pb.h>
#include <yt/yql/providers/dq/common/yql_dq_clique_warmup_config.h>

#include <yql/essentials/core/file_storage/file_storage.h>
#include <yt/yql/providers/dq/actors/yt/resource_manager.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYql {

class IDqWarmupControl: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqWarmupControl>;

    virtual ~IDqWarmupControl() = default;

    virtual bool IsReady(const TMap<TString, TString>& additionalUdfs = {}) = 0;
    virtual void Stop() = 0;
};

using IDqWarmupControlPtr = TIntrusivePtr<IDqWarmupControl>;

class IDqWarmupControlFactory: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqWarmupControlFactory>;

    virtual IDqWarmupControlPtr GetControl() = 0;
    virtual const THashSet<TString>& GetIndexedUdfFilter() = 0;
    virtual bool StripEnabled() const = 0;
};

using IDqWarmupControlFactoryPtr = TIntrusivePtr<IDqWarmupControlFactory>;

IDqWarmupControlFactoryPtr CreateDqWarmupControlFactory(
    const NProto::TDqConfig& config,
    const TMap<TString, TString>& udfs,
    const TFileStoragePtr& fileStorage);

IDqWarmupControlFactoryPtr CreateDqWarmupControlFactory(
    const TString& dqGrpcHost,
    ui32 dqGrpcPort,
    const TString& vanillaJobLite,
    const TString& vanillaJobLiteMd5,
    bool enableStrip,
    bool enableCliqueWarmup,
    const THashSet<TString>& indexedUdfFilter,
    const TMap<TString, TString>& udfs,
    const TFileStoragePtr& fileStorage,
    const TVector<TResourceManagerOptions>& ytBackends);

} // namespace NYql
