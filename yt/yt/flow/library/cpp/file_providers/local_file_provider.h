#pragma once

#include "file_provider_base.h"

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TLocalFileProviderParameters
    : public virtual NYTree::TYsonStruct
{
    std::string Path;

    REGISTER_YSON_STRUCT(TLocalFileProviderParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLocalFileProviderParameters);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLocalFileProvider);

class TLocalFileProvider
    : public TFileProviderBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TLocalFileProviderParameters, TFileProviderBase);

    using TFileProviderBase::TFileProviderBase;

    TFuture<TFileProviderRevisionPtr> Discover() override;

    TFuture<void> Download(
        const TFileProviderRevisionPtr& revision,
        const std::string& stagingDirectory) override;
};

DEFINE_REFCOUNTED_TYPE(TLocalFileProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
