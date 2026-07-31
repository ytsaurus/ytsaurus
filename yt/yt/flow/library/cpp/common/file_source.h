#pragma once

#include "public.h"

#include "spec_validation.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/file_storage/public.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TFileSourceSpec
    : public NYTree::TYsonStruct
{
    // Registered #IFileSource implementation used by this resource.
    std::string FileSourceClassName;
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TFileSourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileSourceSpec);

////////////////////////////////////////////////////////////////////////////////

struct TFileSourceRevision
    : public NYTree::TYsonStruct
{
    // Registered #IFileSource implementation that interprets |Locator|.
    std::string FileSourceClassName;
    // Stable object identity; equal values guarantee byte-identical downloaded files.
    NFileStorage::TFileStorageObjectId ObjectId;
    // Human-readable version for diagnostics; it does not participate in content identity.
    std::string DisplayVersion;
    // Expected downloaded payload size, when the source can determine it during discovery.
    std::optional<i64> Size;
    // Source-specific coordinates of this exact revision.
    NYTree::IMapNodePtr Locator;

    REGISTER_YSON_STRUCT(TFileSourceRevision);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileSourceRevision);

////////////////////////////////////////////////////////////////////////////////

struct TFileSourceContext
    : public TRefCounted
{
    TFileSourceSpecPtr SourceSpec;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr Invoker;
    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TFileSourceContext);

////////////////////////////////////////////////////////////////////////////////

struct IFileSource
    : public virtual TRefCounted
{
    YT_FLOW_REGISTER_PARAMETERS(NYTree::TYsonStruct);

    using TValidator = TNoopSpecValidator;

    virtual TFuture<TFileSourceRevisionPtr> Discover() = 0;

    virtual TFuture<void> Download(
        const TFileSourceRevisionPtr& revision,
        const std::string& stagingDirectory) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
