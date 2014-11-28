#include "common.h"
#include "serialize.h"

#include <numeric>

#include <core/ytree/node.h>

namespace NYT {

namespace {

///////////////////////////////////////////////////////////////////////////////

using NYson::IYsonConsumer;
using NYTree::INodePtr;
using NYTree::ENodeType;

///////////////////////////////////////////////////////////////////////////////

Py::Callable GetYsonType(const std::string& name)
{
    // TODO(ignat): make singleton
    static PyObject* ysonTypesModule = nullptr;
    if (!ysonTypesModule) {
        ysonTypesModule = PyImport_ImportModule("yt.yson.yson_types");
        if (!ysonTypesModule) {
            throw Py::RuntimeError("Failed to import module yt.yson.yson_types");
        }
    }
    return Py::Callable(PyObject_GetAttr(ysonTypesModule, PyString_FromString(name.c_str())));
}

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes)
{
    auto result = GetYsonType(className).apply(Py::TupleN(object));
    result.setAttr("attributes", attributes);
    return result;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NYTree {

///////////////////////////////////////////////////////////////////////////////

void SerializeMapFragment(const Py::Mapping& map, IYsonConsumer* consumer, bool ignoreInnerAttributes, int depth)
{
    auto iterator = Py::Object(PyObject_GetIter(*map), true);
    while (auto* next = PyIter_Next(*iterator)) {
        Py::Object key = Py::Object(next, true);
        char* keyStr = PyString_AsString(ConvertToString(key).ptr());
        auto value = Py::Object(PyMapping_GetItemString(*map, keyStr), true);
        consumer->OnKeyedItem(TStringBuf(keyStr));
        Serialize(value, consumer, ignoreInnerAttributes, depth + 1);
    }
}


void Serialize(const Py::Object& obj, IYsonConsumer* consumer, bool ignoreInnerAttributes, int depth)
{
    const char* attributesStr = "attributes";
    if ((!ignoreInnerAttributes || depth == 0) && PyObject_HasAttrString(*obj, attributesStr)) {
        auto attributes = Py::Mapping(PyObject_GetAttrString(*obj, attributesStr), true);
        if (attributes.length() > 0) {
            consumer->OnBeginAttributes();
            SerializeMapFragment(attributes, consumer, ignoreInnerAttributes, depth);
            consumer->OnEndAttributes();
        }
    }

    if (PyString_Check(obj.ptr())) {
        consumer->OnStringScalar(ConvertToStringBuf(ConvertToString(obj)));
    } else if (obj.isUnicode()) {
        Py::String encoded = Py::String(PyUnicode_AsUTF8String(obj.ptr()), true);
        consumer->OnStringScalar(ConvertToStringBuf(ConvertToString(encoded)));
    } else if (obj.isMapping()) {
        consumer->OnBeginMap();
        SerializeMapFragment(Py::Mapping(obj), consumer, ignoreInnerAttributes, depth);
        consumer->OnEndMap();
    } else if (obj.isSequence()) {
        const auto& objList = Py::Sequence(obj);
        consumer->OnBeginList();
        for (auto it = objList.begin(); it != objList.end(); ++it) {
            consumer->OnListItem();
            Serialize(*it, consumer, ignoreInnerAttributes, depth + 1);
        }
        consumer->OnEndList();
    } else if (obj.isBoolean()) {
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (obj.isInteger() or PyLong_Check(obj.ptr())) {
        if (PyObject_Compare(Py::Long(std::numeric_limits<ui64>::max()).ptr(), obj.ptr()) < 0 ||
            PyObject_Compare(obj.ptr(), Py::Long(std::numeric_limits<i64>::min()).ptr()) < 0)
        {
            throw Py::RuntimeError(
                "Cannot represent in yson " +
                std::string(obj.repr()) +
                " (it if out of range [-2^63, 2^64 - 1])");
        }

        auto longObj = Py::Long(obj);
        if (PyObject_Compare(Py::Int(std::numeric_limits<i64>::max()).ptr(), obj.ptr()) < 0) {
            consumer->OnUint64Scalar(PyLong_AsUnsignedLongLong(longObj.ptr()));
        } else {
            consumer->OnInt64Scalar(PyLong_AsLongLong(longObj.ptr()));
        }
    } else if (obj.isFloat()) {
        consumer->OnDoubleScalar(Py::Float(obj));
    } else if (obj.isNone() || IsInstance(obj, GetYsonType("YsonEntity"))) {
        consumer->OnEntity();
    } else {
        throw Py::RuntimeError(
            "Unsupported python object in tree builder: " +
            std::string(obj.repr()));
    }
}

///////////////////////////////////////////////////////////////////////////////

TPythonObjectBuilder::TPythonObjectBuilder(bool alwaysCreateAttributes)
    : YsonMap(GetYsonType("YsonMap"))
    , YsonList(GetYsonType("YsonList"))
    , YsonString(GetYsonType("YsonString"))
    , YsonInt64(GetYsonType("YsonInt64"))
    , YsonUint64(GetYsonType("YsonUint64"))
    , YsonDouble(GetYsonType("YsonDouble"))
    , YsonBoolean(GetYsonType("YsonBoolean"))
    , YsonEntity(GetYsonType("YsonEntity"))
    , AlwaysCreateAttributes_(alwaysCreateAttributes)
{ }

void TPythonObjectBuilder::OnStringScalar(const TStringBuf& value)
{
    Py::_XDECREF(AddObject(PyString_FromStringAndSize(~value, value.size()), YsonString));
}

void TPythonObjectBuilder::OnInt64Scalar(i64 value)
{
    Py::_XDECREF(AddObject(PyInt_FromLong(value), YsonInt64));
}

void TPythonObjectBuilder::OnUint64Scalar(ui64 value)
{
    Py::_XDECREF(AddObject(PyLong_FromUnsignedLongLong(value), YsonUint64));
}

void TPythonObjectBuilder::OnDoubleScalar(double value)
{
    Py::_XDECREF(AddObject(PyFloat_FromDouble(value), YsonDouble));
}

void TPythonObjectBuilder::OnBooleanScalar(bool value)
{
    Py::_XDECREF(AddObject(PyBool_FromLong(value ? 1 : 0), YsonBoolean));
}

void TPythonObjectBuilder::OnEntity()
{
    Py_INCREF(Py_None);
    Py::_XDECREF(AddObject(Py_None, YsonEntity));
}

void TPythonObjectBuilder::OnBeginList()
{
    auto obj = AddObject(PyList_New(0), YsonList);
    Push(Py::Object(obj, true), EObjectType::List);
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
    auto obj = AddObject(PyDict_New(), YsonMap);
    Push(Py::Object(obj, true), EObjectType::Map);
}

void TPythonObjectBuilder::OnKeyedItem(const TStringBuf& key)
{
    Keys_.push(Stroka(key));
}

void TPythonObjectBuilder::OnEndMap()
{
    Pop();
}

void TPythonObjectBuilder::OnBeginAttributes()
{
    auto obj = Py::Dict();
    Push(obj, EObjectType::Attributes);
}

void TPythonObjectBuilder::OnEndAttributes()
{
    Attributes_ = Pop();
}

PyObject* TPythonObjectBuilder::AddObject(PyObject* obj, const Py::Callable& type)
{
    if (AlwaysCreateAttributes_ && !Attributes_) {
        Attributes_ = Py::Dict();
    }

    if (Attributes_) {
        auto ysonObj = type.apply(Py::TupleN(Py::Object(obj)));
        auto ysonObjPtr = ysonObj.ptr();
        Py::_XDECREF(obj);
        Py::_XINCREF(ysonObjPtr);
        return AddObject(ysonObjPtr);
    } else {
        return AddObject(obj);
    }
}

PyObject* TPythonObjectBuilder::AddObject(const Py::Callable& type)
{
    auto ysonObj = type.apply(Py::Tuple());
    auto ysonObjPtr = ysonObj.ptr();
    Py::_XINCREF(ysonObjPtr);
    return AddObject(ysonObjPtr);
}

PyObject* TPythonObjectBuilder::AddObject(PyObject* obj)
{
    static const char* attributes = "attributes";
    if (Attributes_) {
        PyObject_SetAttrString(obj, const_cast<char*>(attributes), (*Attributes_).ptr());
        Attributes_ = Null;
    }

    if (ObjectStack_.empty()) {
        Objects_.push(Py::Object(obj));
    } else if (ObjectStack_.top().second == EObjectType::List) {
        PyList_Append(ObjectStack_.top().first.ptr(), obj);
    } else {
        auto keyObj = PyString_FromStringAndSize(~Keys_.top(), Keys_.top().size());
        PyDict_SetItem(*ObjectStack_.top().first, keyObj, obj);
        Keys_.pop();
        Py::_XDECREF(keyObj);
    }

    return obj;
}

void TPythonObjectBuilder::Push(const Py::Object& obj, EObjectType objectType)
{
    ObjectStack_.emplace(obj, objectType);
}

Py::Object TPythonObjectBuilder::Pop()
{
    auto obj = ObjectStack_.top().first;
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

///////////////////////////////////////////////////////////////////////////////

void Deserialize(Py::Object& obj, INodePtr node)
{
    Py::Object attributes = Py::Dict();
    if (!node->Attributes().List().empty()) {
        Deserialize(attributes, node->Attributes().ToMap());
    }

    auto type = node->GetType();
    if (type == ENodeType::Map) {
        auto map = Py::Dict();
        for (auto child : node->AsMap()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child.second);
            map.setItem(~child.first, item);
        }
        obj = CreateYsonObject("YsonMap", map, attributes);
    } else if (type == ENodeType::Entity) {
        obj = CreateYsonObject("YsonEntity", Py::None(), attributes);
    } else if (type == ENodeType::Int64) {
        obj = CreateYsonObject("YsonInt64", Py::Int(node->AsInt64()->GetValue()), attributes);
    } else if (type == ENodeType::Uint64) {
        obj = CreateYsonObject("YsonUint64", Py::Long(node->AsUint64()->GetValue()), attributes);
    } else if (type == ENodeType::Double) {
        obj = CreateYsonObject("YsonDouble", Py::Float(node->AsDouble()->GetValue()), attributes);
    } else if (type == ENodeType::String) {
        obj = CreateYsonObject("YsonString", Py::String(~node->AsString()->GetValue()), attributes);
    } else if (type == ENodeType::List) {
        auto list = Py::List();
        for (auto child : node->AsList()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child);
            list.append(item);
        }
        obj = CreateYsonObject("YsonList", list, attributes);
    } else {
        THROW_ERROR_EXCEPTION("Unsupported node type %s", ~ToString(type));
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
