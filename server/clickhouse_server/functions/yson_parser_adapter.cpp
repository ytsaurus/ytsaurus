#include "yson_parser_adapter.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NYTree;

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

void TYsonParserAdapter::preallocate(size_t)
{ }

bool TYsonParserAdapter::parse(const StringRef& yson)
{
    try {
        // TODO(dakovalkov): Remove string copy after YT-11723.
        TYsonString ysonString(yson.data, yson.size);
        Root_ = ConvertToNode(ysonString);
    } catch (const std::exception& /* ex */) {
        return false;
    }
    return true;
}

TYsonParserAdapter::Iterator TYsonParserAdapter::getRoot()
{
    return Iterator{ Root_ };
}


bool TYsonParserAdapter::isInt64(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::Int64;
}

bool TYsonParserAdapter::isUInt64(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::Uint64;
}

bool TYsonParserAdapter::isDouble(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::Double;
}

bool TYsonParserAdapter::isBool(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::Boolean;
}

bool TYsonParserAdapter::isString(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::String;
}

bool TYsonParserAdapter::isArray(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::List;
}

bool TYsonParserAdapter::isObject(const Iterator& it)
{
    return it.Value->GetType() == ENodeType::Map;
}
// We do not support nulls.
bool TYsonParserAdapter::isNull(const Iterator& /* it */)
{
    return false;
}


Int64 TYsonParserAdapter::getInt64(const Iterator& it)
{
    return it.Value->GetValue<i64>();
}

UInt64 TYsonParserAdapter::getUInt64(const Iterator& it)
{
    return it.Value->GetValue<ui64>();
}

double TYsonParserAdapter::getDouble(const Iterator& it)
{
    return it.Value->GetValue<double>();
}

bool TYsonParserAdapter::getBool(const Iterator& it)
{
    return it.Value->GetValue<bool>();
}

StringRef TYsonParserAdapter::getString(const Iterator& it) {
    // GetValue<TString> returns const TString&, so it's ok to provide StringRef outside.
    const auto& result = it.Value->GetValue<TString>();
    return StringRef(result.data(), result.size());
}

// Array part.
size_t TYsonParserAdapter::sizeOfArray(const Iterator& it)
{
    return it.Value->AsList()->GetChildCount();
}

bool TYsonParserAdapter::firstArrayElement(Iterator& it)
{
    return arrayElementByIndex(it, 0);
}

bool TYsonParserAdapter::arrayElementByIndex(Iterator& it, size_t index)
{
    it.IndexInParent = index;
    it.Value = it.Value->AsList()->FindChild(index);
    it.BrothersAndSisters = nullptr;
    return static_cast<bool>(it.Value);
}

bool TYsonParserAdapter::nextArrayElement(Iterator& it)
{
    auto parent = it.Value->GetParent()->AsList();
    if (it.IndexInParent + 1 >= static_cast<size_t>(parent->GetChildCount())) {
        return false;
    }
    ++it.IndexInParent;
    it.Value = parent->GetChild(it.IndexInParent);
    return true;
}

// Object part.
size_t TYsonParserAdapter::sizeOfObject(const Iterator& it)
{
    return it.Value->AsMap()->GetChildCount();
}

bool TYsonParserAdapter::firstObjectMember(Iterator& it)
{
    StringRef _;
    return firstObjectMember(it, _);
}

bool TYsonParserAdapter::firstObjectMember(Iterator& it, StringRef& firstKey)
{
    if (sizeOfObject(it) == 0) {
        return false;
    }
    it.BrothersAndSisters = std::make_shared<std::vector<std::pair<TString, INodePtr>>>(it.Value->AsMap()->GetChildren());
    it.IndexInParent = 0;
    it.Value = (*it.BrothersAndSisters)[0].second;
    const auto& key = (*it.BrothersAndSisters)[0].first;
    firstKey = StringRef(key.data(), key.size());
    return true;
}

bool TYsonParserAdapter::objectMemberByIndex(Iterator& /* it */ , size_t /* index */)
{
    // We do not support get by index for objects since the order in IMapNode is arbitrary.
    return false;
}

bool TYsonParserAdapter::objectMemberByName(Iterator& it, const StringRef& name)
{
    it.BrothersAndSisters = std::make_shared<std::vector<std::pair<TString, INodePtr>>>(it.Value->AsMap()->GetChildren());
    it.IndexInParent = 0;
    for (const auto& [objName, obj] : *it.BrothersAndSisters) {
        if (name == StringRef(objName.data(), objName.size())) {
            it.Value = obj;
            return true;
        }
        ++it.IndexInParent;
    }
    return false;
}

bool TYsonParserAdapter::nextObjectMember(Iterator& it)
{
    StringRef _;
    return nextObjectMember(it, _);
}

bool TYsonParserAdapter::nextObjectMember(Iterator& it, StringRef& nextKey)
{
    if (it.IndexInParent + 1 >= it.BrothersAndSisters->size()) {
        return false;
    }
    ++it.IndexInParent;
    it.Value = (*it.BrothersAndSisters)[it.IndexInParent].second;
    const auto& key = (*it.BrothersAndSisters)[it.IndexInParent].first;
    nextKey = StringRef(key.Data(), key.size());
    return true;
}

bool TYsonParserAdapter::isObjectMember(const Iterator& it)
{
    return static_cast<bool>(it.BrothersAndSisters);
}

StringRef TYsonParserAdapter::getKey(const Iterator& it)
{
    const auto& key = (*it.BrothersAndSisters)[it.IndexInParent].first;
    return StringRef(key.data(), key.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
