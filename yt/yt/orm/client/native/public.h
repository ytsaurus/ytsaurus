#pragma once

#include <yt/yt/orm/client/objects/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/auth/authentication_options.h>

#include <variant>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

using TClientOptions = NAuth::TAuthenticationOptions;

////////////////////////////////////////////////////////////////////////////////

using NObjects::TMasterInstanceTag;

////////////////////////////////////////////////////////////////////////////////

struct TNullPayload;
struct TYsonPayload;
struct TProtobufPayload;

using TPayload = std::variant<
    TNullPayload,
    TYsonPayload,
    TProtobufPayload
>;

inline constexpr TStringBuf OmitPayloadLogFormat = "%n";

using TObjectIdentity = std::variant<
    std::string, // Object key.
    TPayload // Subset of object meta, serialized.
>;

////////////////////////////////////////////////////////////////////////////////

using TAttributeSelector = std::vector<NYPath::TYPath>;

struct TSelectObjectsOptions;
struct TGetObjectOptions;

struct TSetUpdate;
struct TSetRootUpdate;
struct TRemoveUpdate;
struct TLockUpdate;
struct TMethodCall;

using TUpdate = std::variant<
    TSetUpdate,
    TSetRootUpdate,
    TRemoveUpdate,
    TLockUpdate,
    TMethodCall
>;

struct TAttributeTimestampPrerequisite;

struct TUpdateObjectsSubrequest;

struct TUpdateIfExisting;
struct TCreateObjectsSubrequest;

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampResult;

struct TAttributeList;

struct TSelectObjectsResult;
struct TGetObjectResult;
struct TGetObjectsResult;

////////////////////////////////////////////////////////////////////////////////

struct TCreateObjectsSubresult;
struct TUpdateObjectResult;
struct TUpdateObjectsResult;
struct TCreateObjectResult;
struct TCreateObjectsResult;
struct TRemoveObjectResult;
struct TRemoveObjectsResult;

////////////////////////////////////////////////////////////////////////////////

struct TMutatingTransactionOptions;

struct TStartTransactionOptions;
struct TStartTransactionResult;
struct TAbortTransactionResult;
struct TCommitTransactionResult;

struct TAdaptiveBatchSizeOptions;

////////////////////////////////////////////////////////////////////////////////

struct TWatchObjectsResult;

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectHistoryResult;

////////////////////////////////////////////////////////////////////////////////

struct TGetMastersResult;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAdaptiveBatchSizeOptionsConfig)
DECLARE_REFCOUNTED_CLASS(TConnectionConfig)

DECLARE_REFCOUNTED_STRUCT(IChannelPool)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IOrmPeerDiscovery)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPayloadFormat,
    ((None)     (0))
    ((Yson)     (1))
    ((Protobuf) (2))
);

DEFINE_ENUM(EEventType,
    ((ObjectNone)       (0))
    ((ObjectCreated)    (1))
    ((ObjectRemoved)    (2))
    ((ObjectUpdated)    (3))
);

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::TObjectId;
using NClient::NObjects::TTransactionContext;
using NClient::NObjects::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSemaphoreSetGuard)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAggregateMode,
    ((Unspecified) (0))
    ((Aggregate)   (1))
    ((Override)    (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
