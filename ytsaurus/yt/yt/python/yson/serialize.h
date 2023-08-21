#pragma once

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/memory/ref.h>

#include <CXX/Objects.hxx> // pycxx

#include <queue>
#include <optional>
#include <stack>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TPathPart
{
    TStringBuf Key;
    int Index = -1;
    bool InAttributes = false;
};

struct TContext
{
    TCompactVector<TPathPart, 2> PathParts;
    std::optional<size_t> RowIndex;

    void Push(TStringBuf& key)
    {
        TPathPart pathPart;
        pathPart.Key = key;
        PathParts.push_back(pathPart);
    }

    void Push(int index)
    {
        TPathPart pathPart;
        pathPart.Index = index;
        PathParts.push_back(pathPart);
    }

    void PushAttributesStarted()
    {
        TPathPart pathPart;
        pathPart.InAttributes = true;
        PathParts.push_back(pathPart);
    }

    void Pop()
    {
        PathParts.pop_back();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

#if PY_MAJOR_VERSION >= 3
const std::optional<TString> DefaultEncoding = std::make_optional(TString("utf-8"));
#else
const std::optional<TString> DefaultEncoding = std::nullopt;
#endif

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

// This methods allow use methods ConvertTo* with Py::Object.
void Serialize(
    const Py::Object& obj,
    NYson::IYsonConsumer* consumer,
    const std::optional<TString>& encoding = NPython::DefaultEncoding,
    bool ignoreInnerAttributes = false,
    NYson::EYsonType ysonType = NYson::EYsonType::Node,
    bool sortKeys = false,
    int depth = 0,
    TContext* context = nullptr);

void Deserialize(Py::Object& obj, NYTree::INodePtr node, const std::optional<TString>& encoding = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
