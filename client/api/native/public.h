#pragma once

#include <yp/client/api/misc/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/public.h>
#include <yt/core/misc/public.h>
#include <yt/core/ypath/public.h>

#include <variant>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

// Override Arcadia stuff.
using NYT::TIntrusivePtr;
using NYT::TRefCounted;

using NYT::TFuture;

////////////////////////////////////////////////////////////////////////////////

using NYT::NTransactionClient::TTimestamp;
using NYT::NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPayloadFormat,
    ((None)     (0))
    ((Yson)     (1))
    ((Protobuf) (2))
);

struct TNullPayload;
struct TYsonPayload;
struct TProtobufPayload;

using TPayload = std::variant<
    TNullPayload,
    TYsonPayload,
    TProtobufPayload
>;

////////////////////////////////////////////////////////////////////////////////

using TAttributeSelector = std::vector<NYT::NYPath::TYPath>;

struct TSelectObjectsOptions;
struct TGetObjectOptions;

struct TSetUpdate;
struct TRemoveUpdate;

using TUpdate = std::variant<
    TSetUpdate,
    TRemoveUpdate
>;

struct TAttributeTimestampPrerequisite;

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampResult;

struct TAttributeList;

struct TSelectObjectsResult;
struct TGetObjectResult;

////////////////////////////////////////////////////////////////////////////////

struct TUpdateObjectResult;
struct TCreateObjectResult;
struct TRemoveObjectResult;

////////////////////////////////////////////////////////////////////////////////

struct TStartTransactionResult;
struct TAbortTransactionResult;
struct TCommitTransactionResult;

////////////////////////////////////////////////////////////////////////////////

using TUpdateNodeHfsmStateResult = TUpdateObjectResult;

using TRequestPodEvictionResult = TUpdateObjectResult;
using TAbortPodEvictionResult = TUpdateObjectResult;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAuthenticationConfig)
DECLARE_REFCOUNTED_CLASS(TConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TClientConfig)

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
