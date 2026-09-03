#pragma once

#include "file_provider_base.h"

#include <yt/yt/client/cypress_client/public.h>
#include <yt/yt/client/hydra/public.h>
#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/table_client/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYTFileProviderObjectKind,
    ((CypressFile) (0))
    ((BlobTable)   (1))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TYTFileProviderLocator);

struct TYTFileProviderLocator
    : public NYTree::TYsonStruct
{
    std::string Cluster;
    NYPath::TYPath ObjectPath;
    NObjectClient::TObjectId ObjectId;
    NHydra::TRevision Revision;
    EYTFileProviderObjectKind ObjectKind = EYTFileProviderObjectKind::CypressFile;

    REGISTER_YSON_STRUCT(TYTFileProviderLocator);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTFileProviderLocator);

NTableClient::TTableSchemaPtr GetYTFileProviderBlobTableSchema();

TFileProviderRevisionPtr MakeYTFileProviderRevision(
    TStringBuf fileProviderClassName,
    const NYPath::TRichYPath& originalPath,
    const std::string& cluster,
    NObjectClient::TObjectId objectId,
    NHydra::TRevision revision,
    i64 size);

TFileProviderRevisionPtr MakeYTBlobTableFileProviderRevision(
    TStringBuf fileProviderClassName,
    const NYPath::TRichYPath& originalPath,
    const std::string& cluster,
    NObjectClient::TObjectId objectId,
    NHydra::TRevision contentRevision);

TFuture<TFileProviderRevisionPtr> DiscoverYTFileProvider(
    const TFileProviderContextPtr& context,
    TStringBuf fileProviderClassName,
    const NYPath::TRichYPath& path);

TFuture<void> DownloadYTFile(
    const TFileProviderContextPtr& context,
    const TFileProviderRevisionPtr& revision,
    const std::string& stagingDirectory);

////////////////////////////////////////////////////////////////////////////////

struct TYTFileProviderParameters
    : public virtual NYTree::TYsonStruct
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TYTFileProviderParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTFileProviderParameters);

DECLARE_REFCOUNTED_CLASS(TYTFileProvider);

class TYTFileProvider
    : public TFileProviderBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYTFileProviderParameters, TFileProviderBase);

    using TFileProviderBase::TFileProviderBase;

    TFuture<TFileProviderRevisionPtr> Discover() override;

    TFuture<void> Download(
        const TFileProviderRevisionPtr& revision,
        const std::string& stagingDirectory) override;
};

DEFINE_REFCOUNTED_TYPE(TYTFileProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
