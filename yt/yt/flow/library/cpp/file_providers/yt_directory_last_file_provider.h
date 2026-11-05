#pragma once

#include "yt_file_provider.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYTDirectoryLastFileProviderParameters
    : public virtual NYTree::TYsonStruct
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TYTDirectoryLastFileProviderParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTDirectoryLastFileProviderParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYTDirectoryLastFileProviderDynamicParameters
    : public virtual NYTree::TYsonStruct
{
    std::optional<std::string> PinnedFileName;

    REGISTER_YSON_STRUCT(TYTDirectoryLastFileProviderDynamicParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTDirectoryLastFileProviderDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TYTDirectoryLastFileProvider);

class TYTDirectoryLastFileProvider
    : public TFileProviderBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYTDirectoryLastFileProviderParameters, TFileProviderBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TYTDirectoryLastFileProviderDynamicParameters, TFileProviderBase);

    using TFileProviderBase::TFileProviderBase;

    TFuture<TFileProviderRevisionPtr> Discover() override;

    TFuture<void> Download(
        const TFileProviderRevisionPtr& revision,
        const std::string& stagingDirectory) override;
};

DEFINE_REFCOUNTED_TYPE(TYTDirectoryLastFileProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
