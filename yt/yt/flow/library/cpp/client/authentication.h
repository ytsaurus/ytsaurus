#pragma once

#include "public.h"

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <string>
#include <string_view>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// See yt/docs/ru/flow/contributor/internal-authentication.md
// for the request signing scheme and threat model.

//! Method tag embedded in the controller request metadata; guards against replay
//! of a Flow Controller signature against any other YT service.
constexpr std::string_view ControllerRequestMetadataMethod = "FlowExecute";

//! TCustomMetadataExt entry that carries the signature over the serialized TControllerRequestMetadata.
constexpr std::string_view ControllerRequestMetadataSignatureKey = "yt-controller-request-metadata-signature";

//! TCustomMetadataExt entry that marks a request sent to the controller directly, not through the RPC proxy.
constexpr std::string_view DirectRequestMetadataKey = "ytflow-direct";

//! Marks |header| as a direct request: the controller authenticates it by the caller's own
//! YT credentials and authorizes the command itself.
void MarkDirectRequest(NRpc::NProto::TRequestHeader* header);

//! Whether the request is marked as direct. Credentials alone do not tell: the RPC proxy
//! forwards requests with a service ticket of its own.
bool IsDirectRequest(const NRpc::NProto::TRequestHeader& header);

////////////////////////////////////////////////////////////////////////////////

//! Structured metadata an RPC proxy signs when forwarding a Flow Controller
//! RPC request; validated as-is on the Flow Controller side.
struct TControllerRequestMetadata
    : public NYTree::TYsonStruct
{
    std::string Method;

    //! Cypress object id of the addressed pipeline.
    TGuid PipelineObjectId;

    //! Address of the controller leader the request is forwarded to.
    std::string ControllerAddress;

    REGISTER_YSON_STRUCT(TControllerRequestMetadata);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerRequestMetadata)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
