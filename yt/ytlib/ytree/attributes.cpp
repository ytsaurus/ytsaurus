#include "stdafx.h"
#include "attributes.h"
#include "attribute_helpers.h"

#include <ytlib/misc/error.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionary::~IAttributeDictionary()
{ }

TYsonString IAttributeDictionary::GetYson(const Stroka& key) const
{
    const auto& result = FindYson(key);
    if (!result) {
        THROW_ERROR_EXCEPTION("Attribute %s is not found", ~key.Quote());
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
