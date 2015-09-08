#include "stdafx.h"
#include "attributes.h"
#include "attribute_helpers.h"
#include "exception_helpers.h"
#include "ephemeral_node_factory.h"

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionary::~IAttributeDictionary()
{ }

TYsonString IAttributeDictionary::GetYson(const Stroka& key) const
{
    const auto& result = FindYson(key);
    if (!result) {
        ThrowNoSuchAttribute(key);
    }
    return *result;
}

void IAttributeDictionary::MergeFrom(const IMapNodePtr other)
{
    for (const auto& pair : other->GetChildren()) {
        const auto& key = pair.first;
        auto value = ConvertToYsonString(pair.second);
        SetYson(key, value);
    }
}

void IAttributeDictionary::MergeFrom(const IAttributeDictionary& other)
{
    for (const auto& key : other.List()) {
        auto value = other.GetYson(key);
        SetYson(key, value);
    }
}

std::unique_ptr<IAttributeDictionary> IAttributeDictionary::Clone() const
{
    auto attributes = CreateEphemeralAttributes();
    attributes->MergeFrom(*this);
    return attributes;
}

void IAttributeDictionary::Clear()
{
    auto keys = List();
    for (const auto& key : keys) {
        Remove(key);
    }
}

bool IAttributeDictionary::Contains(const Stroka& key) const
{
    return FindYson(key).HasValue();
}

std::unique_ptr<IAttributeDictionary> IAttributeDictionary::FromMap(IMapNodePtr node)
{
    auto attributes = CreateEphemeralAttributes();
    auto children = node->GetChildren();
    for (int i = 0; i < children.size(); ++i) {
        attributes->SetYson(children[i].first, ConvertToYsonString(children[i].second));
    }
    return attributes;
}

IMapNodePtr IAttributeDictionary::ToMap() const
{
    auto map = GetEphemeralNodeFactory()->CreateMap();
    auto keys = List();
    for (const auto& key : keys) {
        map->AddChild(ConvertToNode(GetYson(key)), key);
    }
    return map;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
