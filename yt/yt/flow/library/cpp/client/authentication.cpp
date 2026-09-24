#include "authentication.h"

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TControllerRequestMetadata::Register(TRegistrar registrar)
{
    registrar.Parameter("method", &TThis::Method);
    registrar.Parameter("pipeline_object_id", &TThis::PipelineObjectId);
    registrar.Parameter("controller_address", &TThis::ControllerAddress);
}

////////////////////////////////////////////////////////////////////////////////

void MarkDirectRequest(NRpc::NProto::TRequestHeader* header)
{
    auto* ext = header->MutableExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
    (*ext->mutable_entries())[std::string(DirectRequestMetadataKey)] = "1";
}

bool IsDirectRequest(const NRpc::NProto::TRequestHeader& header)
{
    if (!header.HasExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext)) {
        return false;
    }
    const auto& ext = header.GetExtension(NRpc::NProto::TCustomMetadataExt::custom_metadata_ext);
    return ext.entries().contains(std::string(DirectRequestMetadataKey));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
