#pragma once

#include "yt_file_source.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYTDirectoryLastFileSourceParameters
    : public virtual NYTree::TYsonStruct
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TYTDirectoryLastFileSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTDirectoryLastFileSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYTDirectoryLastFileSourceDynamicParameters
    : public virtual NYTree::TYsonStruct
{
    std::optional<std::string> PinnedFileName;

    REGISTER_YSON_STRUCT(TYTDirectoryLastFileSourceDynamicParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTDirectoryLastFileSourceDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TYTDirectoryLastFileSource);

class TYTDirectoryLastFileSource
    : public TFileSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYTDirectoryLastFileSourceParameters, TFileSourceBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TYTDirectoryLastFileSourceDynamicParameters, TFileSourceBase);

    using TFileSourceBase::TFileSourceBase;

    TFuture<TFileSourceRevisionPtr> Discover() override;

    TFuture<void> Download(
        const TFileSourceRevisionPtr& revision,
        const std::string& stagingDirectory) override;
};

DEFINE_REFCOUNTED_TYPE(TYTDirectoryLastFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
