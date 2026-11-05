#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSourceBase
    : public virtual ISource
{
public:
    TSourceBase(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

    TSourceContextPtr GetContext() const;
    TDynamicSourceContextPtr GetDynamicContext() const;
    TSourceSpecPtr GetSpec() const;
    TDynamicSourceSpecPtr GetDynamicSpec() const;

    // Returns empty partition status.
    NYTree::IMapNodePtr GetPartitionStatus() override;

protected:
    const NLogging::TLogger Logger;

    bool IsDraining() const;

    ISource::TParametersPtr GetParametersBase() const final;
    ISource::TDynamicParametersPtr GetDynamicParametersBase() const final;
    ISource::TDynamicPartitionSpecPtr GetDynamicPartitionSpecBase() const final;

private:
    const TSourceContextPtr Context_;
    const ISource::TParametersPtr Parameters_;

    TAtomicIntrusivePtr<TDynamicSourceContext> DynamicContext_;
    TAtomicIntrusivePtr<ISource::TDynamicParameters> DynamicParameters_;
    TAtomicIntrusivePtr<ISource::TDynamicPartitionSpec> DynamicPartitionSpec_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
