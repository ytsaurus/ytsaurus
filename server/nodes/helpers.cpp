#include "helpers.h"

#include <yp/server/objects/pod.h>

#include <yt/core/ytree/convert.h>

namespace NYP::NServer::NNodes {

using namespace NObjects;

using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

void SchedulePodDynamicAttributesLoad(TPod* pod)
{
    const auto& specDynamicAttributes = pod->Spec().DynamicAttributes().Load();
    pod->Labels().ScheduleLoad();
    for (const auto& key : specDynamicAttributes.annotations()) {
        pod->Annotations().ScheduleLoad(key);
    }
}

NClient::NApi::NProto::TDynamicAttributes BuildPodDynamicAttributes(TPod* pod)
{
    NClient::NApi::NProto::TDynamicAttributes result;

    const auto& specDynamicAttributes = pod->Spec().DynamicAttributes().Load();

    auto labels = pod->Labels().Load();
    for (const auto& [key, child] : labels->GetChildren()) {
        auto* protoItem = result.mutable_labels()->add_attributes();
        protoItem->set_key(key);
        protoItem->set_value(ConvertToYsonString(child).GetData());
    }

    for (const auto& key : specDynamicAttributes.annotations()) {
        auto value = pod->Annotations().Load(key);
        if (value) {
            auto* protoItem = result.mutable_annotations()->add_attributes();
            protoItem->set_key(key);
            protoItem->set_value(value.GetData());
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes

