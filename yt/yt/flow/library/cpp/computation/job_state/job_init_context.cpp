#include "job_init_context.h"

#include <yt/yt/core/misc/error.h>

#include <util/system/type_name.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

ICompanionStateAdapterPtr IJobInitContext::CreateCompanionStateAdapter(TStringBuf name) const
{
    auto context = WithPrefix(name);
    auto manager = context->GetExternalStateManagerOrThrow(context->GetPrefix());
    auto adapter = manager->CreateCompanionAdapter(context->GetPrefix());
    THROW_ERROR_EXCEPTION_UNLESS(adapter,
        "External state manager for state %Qv does not support companion computations",
        context->GetPrefix())
        .With("manager_class", TypeName(*manager));
    return adapter;
}

ICompanionStateAdapterPtr IJobInitContext::CreateJoinedCompanionStateAdapter(TStringBuf name) const
{
    auto context = WithPrefix(name);
    auto joiner = context->GetExternalStateJoinerOrThrow(context->GetPrefix());
    auto adapter = joiner->CreateCompanionAdapter(context->GetPrefix());
    THROW_ERROR_EXCEPTION_UNLESS(adapter,
        "External state joiner for state %Qv does not support companion computations",
        context->GetPrefix())
        .With("joiner_class", TypeName(*joiner));
    return adapter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
