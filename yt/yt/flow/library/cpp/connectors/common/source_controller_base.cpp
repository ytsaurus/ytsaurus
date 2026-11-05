#include "source_controller_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/digest/md5/md5.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSourceControllerBase::TSourceControllerBase(
    TSourceControllerContextPtr context,
    TDynamicSourceControllerContextPtr dynamicContext)
    : Logger(context->Logger)
    , Context_(context)
    , Parameters_(DynamicPointerCast<ISource::TParameters>(TRegistry::Get()->ParseSourceParameters(context->SourceSpec)))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(DynamicPointerCast<ISource::TDynamicParameters>(TRegistry::Get()->ParseDynamicSourceParameters(context->SourceSpec, dynamicContext->DynamicSourceSpec)))
{
    SubscribeReconfigured(BIND([this] (const TDynamicSourceControllerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = DynamicPointerCast<ISource::TDynamicParameters>(TRegistry::Get()->ParseDynamicSourceParameters(Context_->SourceSpec, dynamicContext->DynamicSourceSpec));
    }));
}

TSourceControllerContextPtr TSourceControllerBase::GetContext() const
{
    return Context_;
}

TDynamicSourceControllerContextPtr TSourceControllerBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TSourceSpecPtr TSourceControllerBase::GetSpec() const
{
    return Context_->SourceSpec;
}

TDynamicSourceSpecPtr TSourceControllerBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicSourceSpec;
}

void TSourceControllerBase::Init(IInitContextPtr /*initContext*/)
{ }

void TSourceControllerBase::Sync()
{ }

void TSourceControllerBase::Commit()
{ }

std::string TSourceControllerBase::GetGroup(const TKey& /*key*/)
{
    return "default-group";
}

void TSourceControllerBase::ProcessPartitionStatuses(
    const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& /*statuses*/)
{ }

std::optional<TStreamTraverseDataPtr> TSourceControllerBase::GetFutureKeysStreamTraverseData()
{
    return std::nullopt;
}

ISource::TParametersPtr TSourceControllerBase::GetParametersBase() const
{
    return Parameters_;
}

ISource::TDynamicParametersPtr TSourceControllerBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

std::string MakeSourceIdentity(std::initializer_list<std::string_view> fields)
{
    // Hash the unambiguous yson form: the raw fields cannot be used verbatim, a YT path
    // starts with "//" which is forbidden in state names.
    const auto canonical = NYson::ConvertToYsonString(
        std::vector<std::string>(fields.begin(), fields.end()),
        NYson::EYsonFormat::Text);
    return MD5::Calc(canonical.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
