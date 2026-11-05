#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/source_controller.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSourceControllerBase
    : public virtual ISourceController
{
public:
    TSourceControllerBase(TSourceControllerContextPtr context, TDynamicSourceControllerContextPtr dynamicContext);

    TSourceControllerContextPtr GetContext() const;
    TDynamicSourceControllerContextPtr GetDynamicContext() const;
    TSourceSpecPtr GetSpec() const;
    TDynamicSourceSpecPtr GetDynamicSpec() const;

    void Init(IInitContextPtr initContext) override;
    void Sync() override;
    void Commit() override;

    std::string GetGroup(const TKey& key) override;

    void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses) override;

    std::optional<TStreamTraverseDataPtr> GetFutureKeysStreamTraverseData() override;

protected:
    const NLogging::TLogger Logger;

    ISource::TParametersPtr GetParametersBase() const final;
    ISource::TDynamicParametersPtr GetDynamicParametersBase() const final;

private:
    const TSourceControllerContextPtr Context_;
    const ISource::TParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicSourceControllerContext> DynamicContext_;
    TAtomicIntrusivePtr<ISource::TDynamicParameters> DynamicParameters_;
};

////////////////////////////////////////////////////////////////////////////////

//! Builds the source identity from the given identifying fields: a 128-bit hash of their
//! unambiguous serialized form. Intended for #ISourceController::GetSourceIdentity implementations.
std::string MakeSourceIdentity(std::initializer_list<std::string_view> fields);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
