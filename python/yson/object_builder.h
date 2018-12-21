#pragma once

#include <yt/core/misc/optional.h>
#include <yt/core/misc/ref.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/public.h>

#include <Objects.hxx> // pycxx

#include <queue>
#include <stack>

namespace NYT::NYTree {

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
    TPythonObjectBuilder();
    explicit TPythonObjectBuilder(bool alwaysCreateAttributes, const std::optional<TString>& encoding);

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
    std::optional<TString> Encoding_;

    // NOTE: Not using specific PyCXX objects (e.g. Py::Bytes) here and below to avoid
    // unnecessary checks.
    using PyObjectPtr = std::unique_ptr<PyObject, decltype(&Py::_XDECREF)>;

    std::queue<Py::Object> Objects_;

    std::stack<std::pair<PyObjectPtr, EPythonObjectType>> ObjectStack_;
    // NB(ignat): to avoid using TString we need to make tricky bufferring while reading from input stream.
    std::stack<PyObject*> Keys_;
    std::optional<PyObjectPtr> Attributes_;

    THashMap<TStringBuf, PyObjectPtr> KeyCache_;
    std::vector<PyObjectPtr> OriginalKeyCache_;

    void AddObject(
        PyObjectPtr obj,
        const Py::Callable& type,
        EPythonObjectType objType = EPythonObjectType::Other,
        bool forceYsonTypeCreation = false);

    void Push(PyObjectPtr objPtr, EPythonObjectType objectType);
    PyObjectPtr Pop();

    static PyObjectPtr MakePyObjectPtr(PyObject* obj);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
