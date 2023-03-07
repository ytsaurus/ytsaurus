#include "object_builder.h"

#include <yt/python/common/helpers.h>

namespace NYT::NYTree {

using NPython::GetYsonTypeClass;
using NPython::PyObjectPtr;

////////////////////////////////////////////////////////////////////////////////

TPythonObjectBuilder::TPythonObjectBuilder() = default;

TPythonObjectBuilder::TPythonObjectBuilder(bool alwaysCreateAttributes, const std::optional<TString>& encoding)
    : YsonMap(GetYsonTypeClass("YsonMap"), /* owned */ true)
    , YsonList(GetYsonTypeClass("YsonList"), /* owned */ true)
    , YsonString(GetYsonTypeClass("YsonString"), /* owned */ true)
#if PY_MAJOR_VERSION >= 3
    , YsonUnicode(GetYsonTypeClass("YsonUnicode"), /* owned */ true)
#endif
    , YsonInt64(GetYsonTypeClass("YsonInt64"), /* owned */ true)
    , YsonUint64(GetYsonTypeClass("YsonUint64"), /* owned */ true)
    , YsonDouble(GetYsonTypeClass("YsonDouble"), /* owned */ true)
    , YsonBoolean(GetYsonTypeClass("YsonBoolean"), /* owned */ true)
    , YsonEntity(GetYsonTypeClass("YsonEntity"), /* owned */ true)
    , AlwaysCreateAttributes_(alwaysCreateAttributes)
    , Encoding_(encoding)
    , KeyCache_(/* enable */ true, Encoding_)
{ }

void TPythonObjectBuilder::OnStringScalar(TStringBuf value)
{
    auto bytes = PyObjectPtr(PyBytes_FromStringAndSize(value.data(), value.size()));
    if (!bytes) {
        throw Py::Exception();
    }

    if (Encoding_) {
        auto decodedString = PyObjectPtr(
            PyUnicode_FromEncodedObject(bytes.get(), Encoding_->data(), "strict"));
        if (!decodedString) {
            throw Py::Exception();
        }
#if PY_MAJOR_VERSION < 3
        auto utf8String = PyObjectPtr(PyUnicode_AsUTF8String(decodedString.get()));
        AddObject(std::move(utf8String), YsonString);
#else
        AddObject(std::move(decodedString), YsonUnicode);
#endif
    } else {
        AddObject(std::move(bytes), YsonString);
    }
}

void TPythonObjectBuilder::OnInt64Scalar(i64 value)
{
    AddObject(PyObjectPtr(PyLong_FromLongLong(value)), YsonInt64);
}

void TPythonObjectBuilder::OnUint64Scalar(ui64 value)
{
    AddObject(PyObjectPtr(PyLong_FromUnsignedLongLong(value)), YsonUint64, EPythonObjectType::Other, true);
}

void TPythonObjectBuilder::OnDoubleScalar(double value)
{
    AddObject(PyObjectPtr(PyFloat_FromDouble(value)), YsonDouble);
}

void TPythonObjectBuilder::OnBooleanScalar(bool value)
{
    AddObject(PyObjectPtr(PyBool_FromLong(value ? 1 : 0)), YsonBoolean);
}

void TPythonObjectBuilder::OnEntity()
{
    AddObject(PyObjectPtr(Py::new_reference_to(Py_None)), YsonEntity);
}

void TPythonObjectBuilder::OnBeginList()
{
    AddObject(PyObjectPtr(PyList_New(0)), YsonList, EPythonObjectType::List);
}

void TPythonObjectBuilder::OnListItem()
{
}

void TPythonObjectBuilder::OnEndList()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginMap()
{
    AddObject(PyObjectPtr(PyDict_New()), YsonMap, EPythonObjectType::Map);
}

void TPythonObjectBuilder::OnKeyedItem(TStringBuf key)
{
    Keys_.push(KeyCache_.GetPythonString(key));
}

void TPythonObjectBuilder::OnEndMap()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginAttributes()
{
    Push(PyObjectPtr(PyDict_New()), EPythonObjectType::Attributes);
}

void TPythonObjectBuilder::OnEndAttributes()
{
    Attributes_ = Pop();
}

void TPythonObjectBuilder::AddObject(
    PyObjectPtr obj,
    const Py::Callable& type,
    EPythonObjectType objType,
    bool forceYsonTypeCreation)
{
    static const char* attributesStr = "attributes";

    if (!obj) {
        throw Py::Exception();
    }

    if (Attributes_ || forceYsonTypeCreation || AlwaysCreateAttributes_) {
        auto tuplePtr = PyObjectPtr(PyTuple_New(1));
        PyTuple_SetItem(tuplePtr.get(), 0, Py::new_reference_to(obj.get()));
        obj = PyObjectPtr(PyObject_CallObject(type.ptr(), tuplePtr.get()));
        if (!obj.get()) {
            throw Py::Exception();
        }
    }

    if (Attributes_) {
        PyObject_SetAttrString(obj.get(), attributesStr, Attributes_->get());
        Attributes_ = std::nullopt;
    }

    if (ObjectStack_.empty()) {
        Objects_.push(Py::Object(obj.get()));
    } else if (ObjectStack_.top().second == EPythonObjectType::List) {
        PyList_Append(ObjectStack_.top().first.get(), obj.get());
    } else {
        PyDict_SetItem(ObjectStack_.top().first.get(), Keys_.top().get(), obj.get());
        Keys_.pop();
    }

    if (objType == EPythonObjectType::List || objType == EPythonObjectType::Map) {
        Push(std::move(obj), objType);
    }
}

void TPythonObjectBuilder::Push(PyObjectPtr objPtr, EPythonObjectType objectType)
{
    ObjectStack_.emplace(std::move(objPtr), objectType);
}

PyObjectPtr TPythonObjectBuilder::Pop()
{
    auto obj = std::move(ObjectStack_.top().first);
    ObjectStack_.pop();
    return obj;
}


Py::Object TPythonObjectBuilder::ExtractObject()
{
    auto obj = Objects_.front();
    Objects_.pop();
    return obj;
}

bool TPythonObjectBuilder::HasObject() const
{
    return Objects_.size() > 1 || (Objects_.size() == 1 && ObjectStack_.size() == 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
