#include "merge.h"

#include "attributes.h"
#include "convert.h"

namespace NYT {
namespace NYTree {

namespace {

INodePtr CloneNode(INodePtr node)
{
    return ConvertToNode(node);
}

} // anonymous namespace

INodePtr Update(INodePtr patch, INodePtr base)
{
    if (base->GetType() == ENodeType::Map && patch->GetType() == ENodeType::Map) {
        auto result = CloneNode(base);
        auto resultMap = result->AsMap();
        auto patchMap = patch->AsMap();
        auto baseMap = base->AsMap();
        FOREACH (Stroka key, patchMap->GetKeys()) {
            if (baseMap->FindChild(key)) {
                resultMap->RemoveChild(key);
                resultMap->AddChild(Update(patchMap->GetChild(key), baseMap->GetChild(key)), key);
            }
            else {
                resultMap->AddChild(patchMap->GetChild(key), key);
            }
        }
        result->Attributes().MergeFrom(patch->Attributes());
        return result;
    }
    else {
        auto result = CloneNode(patch);
        result->Attributes().Clear();
        if (base->GetType() == patch->GetType()) {
            result->Attributes().MergeFrom(base->Attributes());
        }
        result->Attributes().MergeFrom(patch->Attributes());
        return result;
    }
}

} // namespace NYTree
} // namespace NYT
