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

} // anonymous namespace

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

    if (IsStringLike(obj)) {
        consumer->OnStringScalar(TStringBuf(PyString_AsString(ConvertToString(obj).ptr())));
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
        consumer->OnStringScalar(Py::Boolean(obj) ? "true" : "false");
    } else if (obj.isInteger()) {
        consumer->OnIntegerScalar(Py::Int(obj).asLongLong());
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

TPythonObjectConsumer::TPythonObjectConsumer()
    : YsonMap(GetYsonType("YsonMap"))
    , YsonList(GetYsonType("YsonList"))
    , YsonString(GetYsonType("YsonString"))
    , YsonInteger(GetYsonType("YsonInteger"))
    , YsonDouble(GetYsonType("YsonDouble"))
    , YsonEntity(GetYsonType("YsonEntity"))
{ }

void TPythonObjectConsumer::OnStringScalar(const TStringBuf& value)
{
    AddObject(Py::String(value), YsonString);
}

void TPythonObjectConsumer::OnIntegerScalar(i64 value)
{
    AddObject(Py::Int(value), YsonInteger);
}

void TPythonObjectConsumer::OnDoubleScalar(double value)
{
    AddObject(Py::Float(value), YsonDouble);
}

void TPythonObjectConsumer::OnEntity()
{
    AddObject(YsonEntity);
}

void TPythonObjectConsumer::OnBeginList()
{
    auto obj = AddObject(Py::List(), YsonList);
    Push(obj, EObjectType::List);
}

void TPythonObjectConsumer::OnListItem()
{
}

void TPythonObjectConsumer::OnEndList()
{
    Pop();
}

void TPythonObjectConsumer::OnBeginMap()
{
    auto obj = AddObject(Py::Dict(), YsonMap);
    Push(obj, EObjectType::Map);
}

void TPythonObjectConsumer::OnKeyedItem(const TStringBuf& key)
{
    Key_ = key;
}

void TPythonObjectConsumer::OnEndMap()
{
    Pop();
    if (ObjectStack_.empty()) {
        Finished_ = true;
    }
}

void TPythonObjectConsumer::OnBeginAttributes()
{
    auto obj = YsonMap.apply(Py::Tuple());
    Push(obj, EObjectType::Attributes);
}

void TPythonObjectConsumer::OnEndAttributes()
{
    Attributes_ = Pop();
}

Py::Object TPythonObjectConsumer::AddObject(const Py::Object& obj, const Py::Callable& type)
{
    if (ObjectStack_.empty() && !Attributes_) {
        Attributes_ = Py::Dict();
    }
    if (Attributes_) {
        auto result = AddObject(type.apply(Py::TupleN(obj)));
        result.setAttr("attributes", *Attributes_);
        Attributes_ = Null;
        return result;
    } else {
        return AddObject(obj);
    }
}

Py::Object TPythonObjectConsumer::AddObject(const Py::Callable& type)
{
    return AddObject(type.apply(Py::Tuple()));
}

Py::Object TPythonObjectConsumer::AddObject(const Py::Object& obj)
{
    if (ObjectStack_.empty()) {
        Objects_.push(obj);
        Finished_ = false;
    } else if (ObjectStack_.top().second == EObjectType::List) {
        PyList_Append(ObjectStack_.top().first.ptr(), *obj);
    } else {
        PyMapping_SetItemString(*ObjectStack_.top().first, const_cast<char*>(~Key_), *obj);
    }
    return obj;
}

void TPythonObjectConsumer::Push(const Py::Object& obj, EObjectType objectType)
{
    ObjectStack_.emplace(obj, objectType);
}

Py::Object TPythonObjectConsumer::Pop()
{
    auto obj = ObjectStack_.top().first;
    ObjectStack_.pop();
    return obj;
}


Py::Object TPythonObjectConsumer::ExtractObject()
{
    auto obj = Objects_.front();
    Objects_.pop();
    return obj;
}

bool TPythonObjectConsumer::HasObject() const
{
    return !Objects_.empty();
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
        FOREACH (auto child, node->AsMap()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child.second);
            map.setItem(~child.first, item);
        }
        obj = CreateYsonObject("YsonMap", map, attributes);
    } else if (type == ENodeType::Entity) {
        obj = CreateYsonObject("YsonEntity", Py::None(), attributes);
    } else if (type == ENodeType::Integer) {
        obj = CreateYsonObject("YsonInteger", Py::Int(node->AsInteger()->GetValue()), attributes);
    } else if (type == ENodeType::Double) {
        obj = CreateYsonObject("YsonDouble", Py::Float(node->AsDouble()->GetValue()), attributes);
    } else if (type == ENodeType::String) {
        obj = CreateYsonObject("YsonString", Py::String(~node->AsString()->GetValue()), attributes);
    } else if (type == ENodeType::List) {
        auto list = Py::List();
        FOREACH (auto child, node->AsList()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child);
            list.append(item);
        }
        obj = CreateYsonObject("YsonList", list, attributes);
    } else {
        THROW_ERROR_EXCEPTION("Unsupported node type %s", ~type.ToString());
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
