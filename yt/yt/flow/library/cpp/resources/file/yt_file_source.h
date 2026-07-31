#pragma once

#include "file_source_base.h"

#include <yt/yt/client/hydra/public.h>
#include <yt/yt/client/object_client/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TYTFileSourceLocator);

struct TYTFileSourceLocator
    : public NYTree::TYsonStruct
{
    std::string Cluster;
    NYPath::TYPath ObjectPath;
    NObjectClient::TObjectId ObjectId;
    NHydra::TRevision Revision;
    std::string Basename;

    REGISTER_YSON_STRUCT(TYTFileSourceLocator);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTFileSourceLocator);

TFileSourceRevisionPtr MakeYTFileSourceRevision(
    TStringBuf fileSourceClassName,
    const NYPath::TRichYPath& originalPath,
    const std::string& cluster,
    NObjectClient::TObjectId objectId,
    NHydra::TRevision revision,
    i64 size,
    const std::string& basename);

TFuture<void> DownloadYTFile(
    const TFileSourceContextPtr& context,
    const TFileSourceRevisionPtr& revision,
    const std::string& stagingDirectory);

////////////////////////////////////////////////////////////////////////////////

struct TYTFileSourceParameters
    : public virtual NYTree::TYsonStruct
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TYTFileSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTFileSourceParameters);

DECLARE_REFCOUNTED_CLASS(TYTFileSource);

class TYTFileSource
    : public TFileSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYTFileSourceParameters, TFileSourceBase);

    using TFileSourceBase::TFileSourceBase;

    TFuture<TFileSourceRevisionPtr> Discover() override;

    TFuture<void> Download(
        const TFileSourceRevisionPtr& revision,
        const std::string& stagingDirectory) override;
};

DEFINE_REFCOUNTED_TYPE(TYTFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
