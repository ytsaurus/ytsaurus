#pragma once

#include <yt/core/misc/optional.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/public.h>

#include <Objects.hxx> // pycxx

#include <queue>
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
    SmallVector<TPathPart, 2> PathParts;
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

namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// This methods allow use methods ConvertTo* with Py::Object.
void Serialize(
    const Py::Object& obj,
    NYson::IYsonConsumer* consumer,
#if PY_MAJOR_VERSION >= 3
    const std::optional<TString>& encoding = std::make_optional(TString("utf-8")),
#else
    const std::optional<TString>& encoding = std::nullopt,
#endif
    bool ignoreInnerAttributes = false,
    NYson::EYsonType ysonType = NYson::EYsonType::Node,
    int depth = 0,
    TContext* context = nullptr);

void Deserialize(Py::Object& obj, NYTree::INodePtr node, const std::optional<TString>& encoding = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

class TGilGuardedYsonConsumer
    : public NYson::TYsonConsumerBase
{
public:
    explicit TGilGuardedYsonConsumer(IYsonConsumer* consumer);

    virtual void OnStringScalar(TStringBuf value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(TStringBuf key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

private:
    IYsonConsumer* Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree

namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
