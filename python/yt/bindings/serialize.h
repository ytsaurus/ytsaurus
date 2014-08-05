#pragma once

#include <contrib/libs/pycxx/Objects.hxx>

#include <core/misc/nullable.h>
#include <core/yson/consumer.h>
#include <core/ytree/public.h>

#include <queue>
#include <stack>

namespace NYT {
namespace NYTree {

///////////////////////////////////////////////////////////////////////////////

// This methods allow use methods convertTo* with Py::Object.
void Serialize(const Py::Object& obj, NYson::IYsonConsumer* consumer);

void Deserialize(Py::Object& obj, NYTree::INodePtr node);

///////////////////////////////////////////////////////////////////////////////

class TPythonObjectBuilder
    : public NYson::TYsonConsumerBase
{
public:
    TPythonObjectBuilder();

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

    bool HasObject() const;
    Py::Object ExtractObject();

private:
    DECLARE_ENUM(EObjectType,
        (Map)
        (List)
        (Attributes)
    );

    Py::Callable YsonMap;
    Py::Callable YsonList;
    Py::Callable YsonString;
    Py::Callable YsonInt64;
    Py::Callable YsonUint64;
    Py::Callable YsonDouble;
    Py::Callable YsonBoolean;
    Py::Callable YsonEntity;

    std::queue<Py::Object> Objects_;

    std::stack<std::pair<Py::Object, EObjectType>> ObjectStack_;
    std::stack<Stroka> Keys_;
    TNullable<Py::Object> Attributes_;

    Py::Object AddObject(const Py::Object& obj, const Py::Callable& type);
    Py::Object AddObject(const Py::Callable& type);
    Py::Object AddObject(Py::Object obj);

    void Push(const Py::Object& obj, EObjectType objectType);
    Py::Object Pop();
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT


