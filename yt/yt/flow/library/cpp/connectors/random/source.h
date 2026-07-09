#pragma once

#include "public.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <util/random/mersenne.h>

#include <random>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TRandomSource
    : public TIntegerOffsetOrderedSourceBase
{
public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicRandomSourceParameters);

    using TSourceController = TRandomSourceController;

    TRandomSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

private:
    TFuture<std::vector<TRecord>> DoReadNextBatch(const TMessageBatcherSettingsPtr& settings, TOffset nextOffset, std::optional<TOffset> offsetLimit) final;

    void DoReportPersistedOffset(TOffset offsetExclusive) final;

private:
    const NTableClient::TTableSchemaPtr Schema_;
    int KeyId_ = 0;
    int DataId_ = 0;

    TMersenne<ui64> Generator_;
};

DEFINE_REFCOUNTED_TYPE(TRandomSource);

////////////////////////////////////////////////////////////////////////////////

class TRandomSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicRandomSourceParameters);

    using TSourceControllerBase::TSourceControllerBase;

    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override;
};

DEFINE_REFCOUNTED_TYPE(TRandomSourceController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
