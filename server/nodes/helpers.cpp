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

    if (!specDynamicAttributes.labels().empty()) {
        pod->Labels().ScheduleLoad();
    }

    for (const auto& key : specDynamicAttributes.annotations()) {
        pod->Annotations().ScheduleLoad(key);
    }
}

NClient::NApi::NProto::TDynamicAttributes BuildPodDynamicAttributes(TPod* pod)
{
    NClient::NApi::NProto::TDynamicAttributes result;

    const auto& specDynamicAttributes = pod->Spec().DynamicAttributes().Load();

    if (!specDynamicAttributes.labels().empty()) {
        auto labels = pod->Labels().Load();
        for (const auto& key : specDynamicAttributes.labels()) {
            auto child = labels->FindChild(key);
            if (child) {
                auto* protoItem = result.mutable_labels()->add_attributes();
                protoItem->set_key(key);
                protoItem->set_value(ConvertToYsonString(child).GetData());
            }
        }
    }

    for (const auto& key : specDynamicAttributes.annotations()) {
        auto optionalValue = pod->Annotations().Load(key);
        if (optionalValue) {
            auto* protoItem = result.mutable_annotations()->add_attributes();
            protoItem->set_key(key);
            protoItem->set_value(optionalValue->GetData());
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes

