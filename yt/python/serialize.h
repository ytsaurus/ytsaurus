#pragma once

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/ref.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/public.h>

#include <contrib/libs/pycxx/Objects.hxx>

#include <queue>
#include <stack>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// This methods allow use methods ConvertTo* with Py::Object.
void Serialize(
    const Py::Object& obj,
    NYson::IYsonConsumer* consumer,
    const TNullable<TString>& encoding = Null,
    bool ignoreInnerAttributes = false,
    NYson::EYsonType ysonType = NYson::EYsonType::Node,
    int depth = 0);

void Deserialize(Py::Object& obj, NYTree::INodePtr node, const TNullable<TString>& encoding = Null);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPythonObjectType,
    (Map)
    (List)
    (Attributes)
    (Other)
);

class TPythonObjectBuilder
    : public NYson::TYsonConsumerBase
{
public:
    explicit TPythonObjectBuilder(bool alwaysCreateAttributes, const TNullable<TString>& encoding);

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
    Py::Callable YsonMap;
    Py::Callable YsonList;
    Py::Callable YsonString;
#if PY_MAJOR_VERSION >= 3
    Py::Callable YsonUnicode;
#endif
    Py::Callable YsonInt64;
    Py::Callable YsonUint64;
    Py::Callable YsonDouble;
    Py::Callable YsonBoolean;
    Py::Callable YsonEntity;

    bool AlwaysCreateAttributes_;
    TNullable<TString> Encoding_;

    std::queue<Py::Object> Objects_;

    std::stack<std::pair<Py::Object, EPythonObjectType>> ObjectStack_;
    // NB(ignat): to avoid using TString we need to make tricky bufferring while reading from input stream.
    std::stack<TString> Keys_;
    TNullable<Py::Object> Attributes_;

    void AddObject(
        PyObject* obj,
        const Py::Callable& type,
        EPythonObjectType objType = EPythonObjectType::Other,
        bool forceYsonTypeCreation = false);
    void AddObject(PyObject* obj);

    void Push(const Py::Object& obj, EPythonObjectType objectType);
    Py::Object Pop();
};

////////////////////////////////////////////////////////////////////////////////

class TGilGuardedYsonConsumer
    : public NYson::TYsonConsumerBase
{
public:
    explicit TGilGuardedYsonConsumer(IYsonConsumer* consumer);

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

private:
    IYsonConsumer* Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

class TListFragmentLexer
{
public:
    TListFragmentLexer();
    explicit TListFragmentLexer(TInputStream* stream);
    ~TListFragmentLexer();
    TListFragmentLexer(TListFragmentLexer&&);
    TListFragmentLexer& operator=(TListFragmentLexer&&);

    TSharedRef NextItem();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree

namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT


