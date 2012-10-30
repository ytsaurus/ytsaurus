#include "stdafx.h"
#include "attributes.h"
#include "attribute_helpers.h"
#include "exception_helpers.h"

namespace NYT {
namespace NYTree {

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
    FOREACH (const auto& pair, other->GetChildren()) {
        const auto& key = pair.first;
        auto value = ConvertToYsonString(pair.second);
        SetYson(key, value);
    }
}

void IAttributeDictionary::MergeFrom(const IAttributeDictionary& other)
{
    FOREACH (const auto& key, other.List()) {
        auto value = other.GetYson(key);
        SetYson(key, value);
    }
}

TAutoPtr<IAttributeDictionary> IAttributeDictionary::Clone() const
{
    auto attributes = CreateEphemeralAttributes();
    attributes->MergeFrom(*this);
    return attributes;
}

void IAttributeDictionary::Clear()
{
    auto keys = List();
    FOREACH (const auto& key, keys) {
        Remove(key);
    }
}

bool IAttributeDictionary::Contains(const Stroka& key) const
{
    return FindYson(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
