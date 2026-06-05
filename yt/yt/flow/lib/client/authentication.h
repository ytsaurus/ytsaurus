#pragma once

#include "public.h"

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <string>
#include <string_view>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// See authentication.md for the request signing scheme and threat model.

//! Method tag embedded in the controller request metadata; guards against replay
//! of a Flow Controller signature against any other YT service.
constexpr std::string_view ControllerRequestMetadataMethod = "FlowExecute";

//! TCustomMetadataExt entry that carries the serialized TControllerRequestMetadata YSON.
constexpr std::string_view ControllerRequestMetadataKey = "yt-controller-request-metadata";

//! TCustomMetadataExt entry that carries the signature over the serialized TControllerRequestMetadata.
constexpr std::string_view ControllerRequestMetadataSignatureKey = "yt-controller-request-metadata-signature";

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
