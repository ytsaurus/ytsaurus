#pragma once

#include "file_source_base.h"

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TLocalFileSourceParameters
    : public virtual NYTree::TYsonStruct
{
    std::string Path;

    REGISTER_YSON_STRUCT(TLocalFileSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLocalFileSourceParameters);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLocalFileSource);

class TLocalFileSource
    : public TFileSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TLocalFileSourceParameters, TFileSourceBase);

    using TFileSourceBase::TFileSourceBase;

    TFuture<TFileSourceRevisionPtr> Discover() override;

    TFuture<void> Download(
        const TFileSourceRevisionPtr& revision,
        const std::string& stagingDirectory) override;
};

DEFINE_REFCOUNTED_TYPE(TLocalFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
