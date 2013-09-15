#include "common.h"
#include "serialize.h"

#include <ytlib/ytree/node.h>

namespace NYT {

namespace {

///////////////////////////////////////////////////////////////////////////////

using NYson::IYsonConsumer;
using NYTree::INodePtr;
using NYTree::ENodeType;

///////////////////////////////////////////////////////////////////////////////

Py::Callable GetYsonType(const std::string& name)
{
    static PyObject* ysonTypesModule = nullptr;
    if (!ysonTypesModule) {
        ysonTypesModule = PyImport_ImportModule("yt.yson.yson_types");
    }
    return Py::Callable(PyObject_GetAttr(ysonTypesModule, PyString_FromString(name.c_str())));
}

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes)
{
    auto result = GetYsonType(className).apply(Py::TupleN(object), Py::Dict());
    result.setAttr("attributes", attributes);
    return result;
}

///////////////////////////////////////////////////////////////////////////////

class TYTreeProducer {
public:
    explicit TYTreeProducer(IYsonConsumer* consumer)
        : Consumer_(consumer)
    { }

    void Process(const Py::Object& obj)
    {
        if (obj.hasAttr("attributes")) {
            auto map = Py::Mapping(GetAttr(obj, "attributes"));
            if (map.length() > 0) {
                Consumer_->OnBeginAttributes();
                ProcessItems(map.items());
                Consumer_->OnEndAttributes();
            }
        }
        // TODO(ignat): maybe use IsInstance in all cases?
        if (obj.isBoolean()) {
            Consumer_->OnStringScalar(Py::Boolean(obj) ? "true" : "false");
        }
        else if (obj.isInteger()) {
            Consumer_->OnIntegerScalar(Py::Int(obj).asLongLong());
        }
        else if (obj.isFloat()) {
            Consumer_->OnDoubleScalar(Py::Float(obj));
        }
        else if (IsStringLike(obj)) {
            Consumer_->OnStringScalar(ConvertToStroka(ConvertToString(obj)));
        }
        else if (IsInstance(obj, GetYsonType("YsonEntity"))) {
            Consumer_->OnEntity();
        }
        else if (obj.isSequence()) {
            const auto& objList = Py::Sequence(obj);
            Consumer_->OnBeginList();
            for (auto it = objList.begin(); it != objList.end(); ++it) {
                Consumer_->OnListItem();
                Process(*it);
            }
            Consumer_->OnEndList();
        }
        else if (obj.isMapping()) {
            Consumer_->OnBeginMap();
            ProcessItems(Py::Mapping(obj).items());
            Consumer_->OnEndMap();
        }
        else {
            throw Py::RuntimeError(
                "Unsupported python object in tree builder: " +
                std::string(obj.repr()));
        }
    }

    void ProcessItems(const Py::List& items)
    {
        // Unfortunately const_iterator doesn't work for mapping,
        // so we use iterator over items
        for (auto it = items.begin(); it != items.end(); ++it) {
            const Py::Tuple& item(*it);
            const auto& key = item.getItem(0);
            const auto& value = item.getItem(1);
            if (!IsStringLike(key)) {
                throw Py::RuntimeError(
                    Sprintf(
                        "Unsupported type (%s) of the key (%s)",
                        std::string(key.type().repr()).c_str(),
                        std::string(key.repr()).c_str()));
            }
            Consumer_->OnKeyedItem(ConvertToStroka(ConvertToString(key)));
            Process(value);
        }
    }

private:
    IYsonConsumer* Consumer_;
};

///////////////////////////////////////////////////////////////////////////////

} // anonymous namespace

namespace NYTree {

///////////////////////////////////////////////////////////////////////////////

void Serialize(const Py::Object& obj, IYsonConsumer* consumer)
{
    TYTreeProducer(consumer).Process(obj);
}

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
    }
    else if (type == ENodeType::Entity) {
        obj = CreateYsonObject("YsonEntity", Py::None(), attributes);
    }
    else if (type == ENodeType::Integer) {
        obj = CreateYsonObject("YsonInteger", Py::Int(node->AsInteger()->GetValue()), attributes);
    }
    else if (type == ENodeType::Double) {
        obj = CreateYsonObject("YsonDouble", Py::Float(node->AsDouble()->GetValue()), attributes);
    }
    else if (type == ENodeType::String) {
        obj = CreateYsonObject("YsonString", Py::String(~node->AsString()->GetValue()), attributes);
    }
    else if (type == ENodeType::List) {
        auto list = Py::List();
        FOREACH (auto child, node->AsList()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child);
            list.append(item);
        }
        obj = CreateYsonObject("YsonList", list, attributes);
    }
    else {
        THROW_ERROR_EXCEPTION("Unsupported node type %s", ~type.ToString());
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
