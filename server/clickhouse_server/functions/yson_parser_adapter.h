#pragma once

#include <yt/core/ytree/public.h>

#include <common/StringRef.h>
#include <Core/Types.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

/// This class can be used as an argument for the template class TFunctionYson.
/// It provides ability to parse YSONs using JSON parser's interface.
struct TYsonParserAdapter
{
    static constexpr bool need_preallocate = false;
    void preallocate(size_t);
    bool parse(const StringRef& yson);

    struct Iterator
    {
        NYTree::INodePtr Value;
        size_t IndexInParent = 0;
        // Children of the parent's map node.
        // We store it here to lock the order and to return StringRef on keys in some methods.
        std::shared_ptr<std::vector<std::pair<TString, NYTree::INodePtr>>> BrothersAndSisters = nullptr;
    };

    Iterator getRoot();

    static bool isInt64(const Iterator& it);
    static bool isUInt64(const Iterator& it);
    static bool isDouble(const Iterator& it);
    static bool isBool(const Iterator& it);
    static bool isString(const Iterator& it);
    static bool isArray(const Iterator& it);
    static bool isObject(const Iterator& it);
    // We do not support nulls.
    static bool isNull(const Iterator& /* it */);


    static DB::Int64 getInt64(const Iterator& it);
    static DB::UInt64 getUInt64(const Iterator& it);
    static double getDouble(const Iterator& it);
    static bool getBool(const Iterator& it);
    static StringRef getString(const Iterator& it);

    // Array part.
    static size_t sizeOfArray(const Iterator& it);
    static bool firstArrayElement(Iterator& it);
    static bool arrayElementByIndex(Iterator& it, size_t index);
    static bool nextArrayElement(Iterator& it);

    // Object part.
    static size_t sizeOfObject(const Iterator& it);
    static bool firstObjectMember(Iterator& it);
    static bool firstObjectMember(Iterator& it, StringRef& firstKey);
    // We do not support get by index for objects since the order in IMapNode is arbitrary.
    static bool objectMemberByIndex(Iterator& /* it */ , size_t /* index */);
    static bool objectMemberByName(Iterator& it, const StringRef& name);
    static bool nextObjectMember(Iterator& it);
    static bool nextObjectMember(Iterator& it, StringRef& nextKey);
    static bool isObjectMember(const Iterator& it);
    static StringRef getKey(const Iterator& it);

private:
    NYTree::INodePtr Root_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
