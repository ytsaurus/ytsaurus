#include "object_builder.h"
#include "helpers.h"

namespace NYT {
namespace NYTree {

using NPython::GetYsonTypeClass;

////////////////////////////////////////////////////////////////////////////////

TPythonObjectBuilder::TPythonObjectBuilder() = default;

TPythonObjectBuilder::TPythonObjectBuilder(bool alwaysCreateAttributes, const TNullable<TString>& encoding)
    : YsonMap(GetYsonTypeClass("YsonMap"))
    , YsonList(GetYsonTypeClass("YsonList"))
    , YsonString(GetYsonTypeClass("YsonString"))
#if PY_MAJOR_VERSION >= 3
    , YsonUnicode(GetYsonTypeClass("YsonUnicode"))
#endif
    , YsonInt64(GetYsonTypeClass("YsonInt64"))
    , YsonUint64(GetYsonTypeClass("YsonUint64"))
    , YsonDouble(GetYsonTypeClass("YsonDouble"))
    , YsonBoolean(GetYsonTypeClass("YsonBoolean"))
    , YsonEntity(GetYsonTypeClass("YsonEntity"))
    , AlwaysCreateAttributes_(alwaysCreateAttributes)
    , Encoding_(encoding)
{ }

TPythonObjectBuilder::PyObjectPtr TPythonObjectBuilder::MakePyObjectPtr(PyObject* obj)
{
    return std::unique_ptr<PyObject, decltype(&Py::_XDECREF)>(obj, &Py::_XDECREF);
}

void TPythonObjectBuilder::OnStringScalar(const TStringBuf& value)
{
    auto bytes = MakePyObjectPtr(PyBytes_FromStringAndSize(~value, value.size()));
    if (!bytes) {
        throw Py::Exception();
    }

    if (Encoding_) {
        auto decodedString = MakePyObjectPtr(
            PyUnicode_FromEncodedObject(bytes.get(), ~Encoding_.Get(), "strict"));
        if (!decodedString) {
            throw Py::Exception();
        }
#if PY_MAJOR_VERSION < 3
        auto utf8String = MakePyObjectPtr(PyUnicode_AsUTF8String(decodedString.get()));
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
    AddObject(MakePyObjectPtr(PyLong_FromLongLong(value)), YsonInt64);
}

void TPythonObjectBuilder::OnUint64Scalar(ui64 value)
{
    AddObject(MakePyObjectPtr(PyLong_FromUnsignedLongLong(value)), YsonUint64, EPythonObjectType::Other, true);
}

void TPythonObjectBuilder::OnDoubleScalar(double value)
{
    AddObject(MakePyObjectPtr(PyFloat_FromDouble(value)), YsonDouble);
}

void TPythonObjectBuilder::OnBooleanScalar(bool value)
{
    AddObject(MakePyObjectPtr(PyBool_FromLong(value ? 1 : 0)), YsonBoolean);
}

void TPythonObjectBuilder::OnEntity()
{
    AddObject(MakePyObjectPtr(Py::new_reference_to(Py_None)), YsonEntity);
}

void TPythonObjectBuilder::OnBeginList()
{
    AddObject(MakePyObjectPtr(PyList_New(0)), YsonList, EPythonObjectType::List);
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
    AddObject(MakePyObjectPtr(PyDict_New()), YsonMap, EPythonObjectType::Map);
}

void TPythonObjectBuilder::OnKeyedItem(const TStringBuf& key)
{
    PyObject* pyKey = nullptr;

    auto it = KeyCache_.find(key);
    if (it == KeyCache_.end()) {
        auto pyKeyPtr = MakePyObjectPtr(PyBytes_FromStringAndSize(~key, key.size()));
        if (!pyKeyPtr) {
            throw Py::Exception();
        }

        auto ownedKey = ConvertToStringBuf(Py::Bytes(pyKeyPtr.get()));

        if (Encoding_) {
            auto originalPyKeyObj = pyKeyPtr.get();
            OriginalKeyCache_.emplace_back(std::move(pyKeyPtr));

            pyKeyPtr = MakePyObjectPtr(
                PyUnicode_FromEncodedObject(originalPyKeyObj, ~Encoding_.Get(), "strict"));
            if (!pyKeyPtr) {
                throw Py::Exception();
            }
        }

        auto res = KeyCache_.emplace(ownedKey, std::move(pyKeyPtr));
        YCHECK(res.second);
        pyKey = res.first->second.get();
    } else {
        pyKey = it->second.get();
    }

    Keys_.push(pyKey);
}

void TPythonObjectBuilder::OnEndMap()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginAttributes()
{
    Push(MakePyObjectPtr(PyDict_New()), EPythonObjectType::Attributes);
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
        auto tuplePtr = MakePyObjectPtr(PyTuple_New(1));
        PyTuple_SetItem(tuplePtr.get(), 0, Py::new_reference_to(obj.get()));
        obj = MakePyObjectPtr(PyObject_CallObject(type.ptr(), tuplePtr.get()));
        if (!obj.get()) {
            throw Py::Exception();
        }
    }

    if (Attributes_) {
        PyObject_SetAttrString(obj.get(), attributesStr, Attributes_->get());
        Attributes_ = Null;
    }

    if (ObjectStack_.empty()) {
        Objects_.push(Py::Object(obj.get()));
    } else if (ObjectStack_.top().second == EPythonObjectType::List) {
        PyList_Append(ObjectStack_.top().first.get(), obj.get());
    } else {
        PyDict_SetItem(ObjectStack_.top().first.get(), Keys_.top(), obj.get());
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

TPythonObjectBuilder::PyObjectPtr TPythonObjectBuilder::Pop()
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

} // namespace NYTree
} // namespace NYT
