#include "common.h"
#include "serialize.h"

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

void SerializeMapFragment(const Py::Mapping& map, IYsonConsumer* consumer)
{
    auto iterator = Py::Object(PyObject_GetIter(*map), true);
    while (auto* next = PyIter_Next(*iterator)) {
        Py::Object key = Py::Object(next, true);
        char* keyStr = PyString_AsString(ConvertToString(key).ptr());
        auto value = Py::Object(PyMapping_GetItemString(*map, keyStr), true);
        consumer->OnKeyedItem(TStringBuf(keyStr));
        Serialize(value, consumer);
    }
}


void Serialize(const Py::Object& obj, IYsonConsumer* consumer)
{
    const char* attributesStr = "attributes";
    if (PyObject_HasAttrString(*obj, attributesStr)) {
        auto attributes = Py::Mapping(PyObject_GetAttrString(*obj, attributesStr), true);
        if (attributes.length() > 0) {
            consumer->OnBeginAttributes();
            SerializeMapFragment(attributes, consumer);
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
        SerializeMapFragment(Py::Mapping(obj), consumer);
        consumer->OnEndMap();
    } else if (obj.isSequence()) {
        const auto& objList = Py::Sequence(obj);
        consumer->OnBeginList();
        for (auto it = objList.begin(); it != objList.end(); ++it) {
            consumer->OnListItem();
            Serialize(*it, consumer);
        }
        consumer->OnEndList();
    } else if (obj.isBoolean()) {
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (obj.isInteger()) {
        consumer->OnInt64Scalar(Py::Int(obj).asLongLong());
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

TPythonObjectBuilder::TPythonObjectBuilder()
    : YsonMap(GetYsonType("YsonMap"))
    , YsonList(GetYsonType("YsonList"))
    , YsonString(GetYsonType("YsonString"))
    , YsonInt64(GetYsonType("YsonInt64"))
    , YsonUint64(GetYsonType("YsonUint64"))
    , YsonDouble(GetYsonType("YsonDouble"))
    , YsonBoolean(GetYsonType("YsonBoolean"))
    , YsonEntity(GetYsonType("YsonEntity"))
{ }

void TPythonObjectBuilder::OnStringScalar(const TStringBuf& value)
{
    AddObject(Py::String(value), YsonString);
}

void TPythonObjectBuilder::OnInt64Scalar(i64 value)
{
    AddObject(Py::Int(value), YsonInt64);
}

void TPythonObjectBuilder::OnUint64Scalar(ui64 value)
{
    AddObject(Py::Long(value), YsonUint64);
}

void TPythonObjectBuilder::OnDoubleScalar(double value)
{
    AddObject(Py::Float(value), YsonDouble);
}

void TPythonObjectBuilder::OnBooleanScalar(bool value)
{
    AddObject(Py::Boolean(value), YsonBoolean);
}

void TPythonObjectBuilder::OnEntity()
{
    AddObject(YsonEntity);
}

void TPythonObjectBuilder::OnBeginList()
{
    auto obj = AddObject(Py::List(), YsonList);
    Push(obj, EObjectType::List);
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
    auto obj = AddObject(Py::Dict(), YsonMap);
    Push(obj, EObjectType::Map);
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
    auto obj = YsonMap.apply(Py::Tuple());
    Push(obj, EObjectType::Attributes);
}

void TPythonObjectBuilder::OnEndAttributes()
{
    Attributes_ = Pop();
}

Py::Object TPythonObjectBuilder::AddObject(const Py::Object& obj, const Py::Callable& type)
{
    if (Attributes_) {
        return AddObject(type.apply(Py::TupleN(obj)));
    } else {
        return AddObject(obj);
    }
}

Py::Object TPythonObjectBuilder::AddObject(const Py::Callable& type)
{
    return AddObject(type.apply(Py::Tuple()));
}

Py::Object TPythonObjectBuilder::AddObject(Py::Object obj)
{
    if (ObjectStack_.empty()) {
        Objects_.push(obj);
    } else if (ObjectStack_.top().second == EObjectType::List) {
        PyList_Append(ObjectStack_.top().first.ptr(), *obj);
    } else {
        PyMapping_SetItemString(*ObjectStack_.top().first, const_cast<char*>(~Keys_.top()), *obj);
        Keys_.pop();
    }
    if (Attributes_) {
        obj.setAttr("attributes", *Attributes_);
        Attributes_ = Null;
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
