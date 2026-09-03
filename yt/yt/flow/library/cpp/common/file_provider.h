#pragma once

#include "public.h"

#include "spec_validation.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/file_storage/public.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void ValidateFileProviderName(TStringBuf name);

////////////////////////////////////////////////////////////////////////////////

struct TFileProviderSpec
    : public NYTree::TYsonStruct
{
    // Registered #IFileProvider implementation used by this resource.
    std::string FileProviderClassName;
    NYTree::IMapNodePtr Parameters;
    std::optional<std::string> PostprocessCommand;
    TDuration PostprocessTimeout;

    REGISTER_YSON_STRUCT(TFileProviderSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileProviderSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicFileProviderSpec
    : public NYTree::TYsonStruct
{
    // Provider-specific parameters used to select future revisions during discovery.
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TDynamicFileProviderSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicFileProviderSpec);

////////////////////////////////////////////////////////////////////////////////

struct TFileProviderRevision
    : public NYTree::TYsonStruct
{
    // Registered #IFileProvider implementation that interprets |Locator|.
    std::string FileProviderClassName;
    // Stable object identity; equal values guarantee byte-identical downloaded files.
    NFileStorage::TFileStorageObjectId ObjectId;
    // Human-readable version for diagnostics; it does not participate in content identity.
    std::string DisplayVersion;
    // Expected downloaded payload size, when the provider can determine it during discovery.
    std::optional<i64> Size;
    // Provider-specific coordinates of this exact revision.
    NYTree::IMapNodePtr Locator;

    REGISTER_YSON_STRUCT(TFileProviderRevision);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileProviderRevision);

////////////////////////////////////////////////////////////////////////////////

struct TFileProviderContext
    : public TRefCounted
{
    TFileProviderSpecPtr ProviderSpec;
    IPipelineAuthenticatorPtr PipelineAuthenticator;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NHttp::IClientPtr HttpClient;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr Invoker;
    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TFileProviderContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicFileProviderContext
    : public TRefCounted
{
    TDynamicFileProviderSpecPtr DynamicFileProviderSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicFileProviderContext);

////////////////////////////////////////////////////////////////////////////////

struct IFileProvider
    : public virtual TRefCounted
    , public virtual TReconfigurable<TDynamicFileProviderContext>
{
    YT_FLOW_REGISTER_PARAMETERS(NYTree::TYsonStruct);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(NYTree::TYsonStruct);

    using TValidator = TNoopSpecValidator;

    virtual TFuture<TFileProviderRevisionPtr> Discover() = 0;

    // Materializes exactly |revision|; current dynamic parameters must not reselect its version.
    virtual TFuture<void> Download(
        const TFileProviderRevisionPtr& revision,
        const std::string& stagingDirectory) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
