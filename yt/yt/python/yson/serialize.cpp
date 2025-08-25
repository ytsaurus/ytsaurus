#include "serialize.h"
#include "yson_lazy_map.h"
#include "helpers.h"
#include "error.h"

#include "skiff/other_columns.h"

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/attributes.h>

#include <util/system/sanitizers.h>
#include <util/generic/typetraits.h>

namespace NYT {

using NYson::EYsonType;
using NYson::IYsonConsumer;
using NYTree::INodePtr;
using NYTree::ENodeType;
using NPython::GetYsonTypeClass;
using NPython::FindYsonTypeClass;
using NPython::EncodeStringObject;
using NPython::CreateYsonError;

namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object CreateYsonObject(const std::string& className, const Py::Object& object, const Py::Object& attributes)
{
    auto result = Py::Callable(GetYsonTypeClass(className), /* owned */ true).apply(Py::TupleN(object));
    result.setAttr("attributes", attributes);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython

namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

static void ValidateKeyType(const Py::Object& key, TContext* context = nullptr)
{
    static auto* YsonStringProxyClass = FindYsonTypeClass("YsonStringProxy");

    if (!PyBytes_Check(key.ptr()) &&
        !PyUnicode_Check(key.ptr()) &&
        !(YsonStringProxyClass && PyObject_IsInstance(key.ptr(), YsonStringProxyClass)))
    {
        if (context) {
            throw CreateYsonError(Format("Map key should be string, found %Qv", Py::Repr(key)), context);
        } else {
            throw Py::RuntimeError(Format("Map key should be string, found %Qv", Py::Repr(key)));
        }
    }
}

void SerializeLazyMapFragment(
    const Py::Object& map,
    IYsonConsumer* consumer,
    const std::optional<TString>& encoding,
    bool ignoreInnerAttributes,
    EYsonType ysonType,
    bool sortKeys,
    int depth,
    TContext* context)
{
    if (sortKeys) {
        throw Py::RuntimeError("sort_keys=True is not implemented for lazy map fragment");
    }

    TLazyYsonMapBase* obj = reinterpret_cast<TLazyYsonMapBase*>(map.ptr());
    for (const auto& item : *obj->Dict->GetUnderlyingHashMap()) {
        const auto& key = item.first;
        const auto& value = item.second;
        ValidateKeyType(key);

        auto encodedKey = EncodeStringObject(key, encoding, context);
        auto mapKey = ConvertToStringBuf(encodedKey);
        consumer->OnKeyedItem(mapKey);
        context->Push(mapKey);

        if (value.Value) {
            Serialize(*value.Value, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth + 1);
        } else {
            std::visit([&] (auto&& data) {
                using T = std::decay_t<decltype(data)>;
                if constexpr (std::is_same_v<T, TSharedRef>) {
                    consumer->OnRaw(TStringBuf(data.Begin(), data.Size()), NYson::EYsonType::Node);
                } else if constexpr (std::is_same_v<T, NYson::TYsonItem>) {
                    switch (data.GetType()) {
                        case NYson::EYsonItemType::EntityValue:
                            consumer->OnEntity();
                            break;
                        case NYson::EYsonItemType::BooleanValue:
                            consumer->OnBooleanScalar(data.UncheckedAsBoolean());
                            break;
                        case NYson::EYsonItemType::Int64Value:
                            consumer->OnInt64Scalar(data.UncheckedAsInt64());
                            break;
                        case NYson::EYsonItemType::Uint64Value:
                            consumer->OnUint64Scalar(data.UncheckedAsUint64());
                            break;
                        case NYson::EYsonItemType::DoubleValue:
                            consumer->OnDoubleScalar(data.UncheckedAsDouble());
                            break;
                        default:
                            YT_ABORT();
                    }
                } else {
                    static_assert(TDependentFalse<T>, "non-exhaustive visitor!");
                }
            },
            value.Data);
        }
        context->Pop();
    }
}

void SerializeMapFragment(
    const Py::Object& map,
    IYsonConsumer* consumer,
    const std::optional<TString> &encoding,
    bool ignoreInnerAttributes,
    EYsonType ysonType,
    bool sortKeys,
    int depth,
    TContext* context)
{
    if (IsYsonLazyMap(map.ptr())) {
        SerializeLazyMapFragment(map, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth, context);
        return;
    }

    auto onItem = [&] (PyObject* item) {
        auto itemGuard = Finally([item] { Py::_XDECREF(item); });

        auto key = Py::Object(PyTuple_GetItem(item, 0), false);
        auto value = Py::Object(PyTuple_GetItem(item, 1), false);
        ValidateKeyType(key, context);

        auto encodedKey = EncodeStringObject(key, encoding, context);
        auto mapKey = ConvertToStringBuf(encodedKey);
        consumer->OnKeyedItem(mapKey);
        context->Push(mapKey);
        Serialize(value, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth + 1, context);
        context->Pop();
    };

    const char* itemsKeyword = "items";
    auto items = Py::Object(
        PyDict_CheckExact(*map) ? PyDict_Items(*map) : PyObject_CallMethod(*map, const_cast<char*>(itemsKeyword), nullptr),
        true);
    auto iterator = CreateIterator(items);

    if (sortKeys) {
        std::vector<std::pair<TString, PyObject*>> itemsSortedByKey;

        while (auto* item = PyIter_Next(*iterator)) {
            auto key = Py::Object(PyTuple_GetItem(item, 0), false);
            ValidateKeyType(key, context);
            auto encodedKey = EncodeStringObject(key, encoding, context);
            auto mapKey = ConvertToStringBuf(encodedKey);
            itemsSortedByKey.emplace_back(mapKey, item);
        }

        std::sort(itemsSortedByKey.begin(), itemsSortedByKey.end());
        for (const auto& [_, object] : itemsSortedByKey) {
            onItem(object);
        }
    } else {
        while (auto* item = PyIter_Next(*iterator)) {
            onItem(item);
        }
    }
}

void SerializePythonInteger(const Py::Object& obj, IYsonConsumer* consumer, TContext* context)
{
    static auto* YsonBooleanClass = GetYsonTypeClass("YsonBoolean");
    static auto* YsonUint64Class = GetYsonTypeClass("YsonUint64");
    static auto* YsonInt64Class = GetYsonTypeClass("YsonInt64");

    // TODO(asaitgalin): Make singleton with all global variables and
    // free all objects there before interpreter exit.
    static auto* SignedInt64Min = PyLong_FromLongLong(std::numeric_limits<i64>::min());
    static auto* SignedInt64Max = PyLong_FromLongLong(std::numeric_limits<i64>::max());
    static auto* UnsignedInt64Max = PyLong_FromUnsignedLongLong(std::numeric_limits<ui64>::max());

    if (PyObject_RichCompareBool(UnsignedInt64Max, obj.ptr(), Py_LT) == 1 ||
        PyObject_RichCompareBool(obj.ptr(), SignedInt64Min, Py_LT) == 1)
    {
        throw CreateYsonError(
            Format(
                "Integer %v cannot be serialized to YSON since it is out of range [-2^63, 2^64 - 1]",
                Py::Repr(obj)),
            context);
    }

    auto consumeAsLong = [&] {
        int greaterThanInt64 = PyObject_RichCompareBool(SignedInt64Max, obj.ptr(), Py_LT);

        if (greaterThanInt64 == 1) {
            auto value = PyLong_AsUnsignedLongLong(obj.ptr());
            if (PyErr_Occurred()) {
                throw Py::Exception();
            }
            consumer->OnUint64Scalar(value);
        } else if (greaterThanInt64 == 0) {
            auto value = PyLong_AsLongLong(obj.ptr());
            if (PyErr_Occurred()) {
                throw Py::Exception();
            }
            consumer->OnInt64Scalar(value);
        } else {
            YT_ABORT();
        }
    };

    if (PyLong_CheckExact(obj.ptr())) {
        consumeAsLong();
    } else if (PyObject_IsInstance(obj.ptr(), YsonBooleanClass)) {
        // YsonBoolean inherited from int
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (PyObject_IsInstance(obj.ptr(), YsonUint64Class)) {
        auto value = static_cast<ui64>(Py::LongLong(obj));
        if (PyErr_Occurred()) {
            PyErr_Clear();
            throw CreateYsonError("Can not dump negative integer as YSON uint64", context);
        }
        consumer->OnUint64Scalar(value);
    } else if (PyObject_IsInstance(obj.ptr(), YsonInt64Class)) {
        auto value = static_cast<i64>(Py::LongLong(obj));
        if (PyErr_Occurred()) {
            PyErr_Clear();
            throw CreateYsonError("Can not dump integer as YSON int64", context);
        }
        consumer->OnInt64Scalar(value);
    } else {
        // Object 'obj' is of some type inhereted from integer.
        consumeAsLong();
    }
}

PyObject* PyStringFromString(const char* str)
{
#if PY_MAJOR_VERSION < 3
    return PyString_FromString(str);
#else
    return PyUnicode_FromString(str);
#endif
}

bool HasAttributes(const Py::Object& obj)
{
    const char* hasAttributesStr = "has_attributes";
    static auto* hasAttributesPyStr = PyStringFromString(hasAttributesStr);
    static auto* attributesPyStr = PyStringFromString("attributes");
    if (PyObject_HasAttr(obj.ptr(), hasAttributesPyStr)) {
        return Py::Boolean(obj.callMemberFunction(hasAttributesStr));
    }
    return PyObject_HasAttr(obj.ptr(), attributesPyStr);
}

bool HasCallableToYsonType(const Py::Object& obj)
{
    static auto* toYsonTypePyStr = PyStringFromString("to_yson_type");
    return PyObject_HasAttr(obj.ptr(), toYsonTypePyStr) && obj.getAttr("to_yson_type").isCallable();
}

void Serialize(
    const Py::Object& obj,
    IYsonConsumer* consumer,
    const std::optional<TString>& encoding,
    bool ignoreInnerAttributes,
    EYsonType ysonType,
    bool sortKeys,
    int depth,
    TContext* context)
{
    static auto* YsonEntityClass = GetYsonTypeClass("YsonEntity");
    static auto* YsonStringProxyClass = FindYsonTypeClass("YsonStringProxy");

    std::unique_ptr<TContext> contextHolder;
    if (!context) {
        contextHolder.reset(new TContext());
        context = contextHolder.get();
    }

    const char* attributesStr = "attributes";
    if ((!ignoreInnerAttributes || depth == 0) && HasAttributes(obj)) {
        if (HasCallableToYsonType(obj)) {
            auto repr = obj.callMemberFunction("to_yson_type");
            Serialize(repr, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth, context);
            return;
        }
        auto attributeObject = obj.getAttr(attributesStr);
        if ((!attributeObject.isMapping() && !attributeObject.isNone()) || attributeObject.isSequence())  {
            throw CreateYsonError("Invalid field 'attributes', it is neither mapping nor None", context);
        }
        if (!attributeObject.isNone()) {
            auto attributes = Py::Mapping(attributeObject);
            if (attributes.length() > 0) {
                consumer->OnBeginAttributes();
                context->PushAttributesStarted();
                SerializeMapFragment(attributes, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth, context);
                context->Pop();
                consumer->OnEndAttributes();
            }
        }
    }

    if (PyBytes_Check(obj.ptr()) || PyUnicode_Check(obj.ptr())) {
        auto encodedObj = EncodeStringObject(obj, encoding, context);
        consumer->OnStringScalar(ConvertToStringBuf(encodedObj));
#if PY_MAJOR_VERSION < 3
    // Fast check for simple integers (python 3 has only long integers)
    } else if (PyInt_CheckExact(obj.ptr())) {
        consumer->OnInt64Scalar(Py::ConvertToLongLong(obj));
#endif
    } else if (obj.isBoolean()) {
        consumer->OnBooleanScalar(Py::Boolean(obj));
    } else if (Py::IsInteger(obj)) {
        SerializePythonInteger(obj, consumer, context);
    } else if (YsonStringProxyClass && Py_TYPE(obj.ptr()) == reinterpret_cast<PyTypeObject*>(YsonStringProxyClass)) {
        consumer->OnStringScalar(ConvertToStringBuf(obj.getAttr("_bytes")));
    } else if (Py_TYPE(obj.ptr()) == NPython::TSkiffOtherColumns::type_object()) {
        Py::PythonClassObject<NPython::TSkiffOtherColumns> skiffOtherColumns(obj);
        consumer->OnRaw(skiffOtherColumns.getCxxObject()->GetYsonString());
    } else if (obj.isMapping() && obj.hasAttr("items") || IsYsonLazyMap(obj.ptr())) {
        bool allowBeginEnd =  depth > 0 || ysonType != NYson::EYsonType::MapFragment;
        if (allowBeginEnd) {
            consumer->OnBeginMap();
        }
        SerializeMapFragment(obj, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth, context);
        if (allowBeginEnd) {
            consumer->OnEndMap();
        }
    } else if (obj.isSequence()) {
        const auto& objList = Py::Sequence(obj);
        consumer->OnBeginList();
        int index = 0;
        for (auto it = objList.begin(); it != objList.end(); ++it) {
            consumer->OnListItem();
            context->Push(index);
            Serialize(*it, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth + 1, context);
            context->Pop();
            ++index;
        }
        consumer->OnEndList();
    } else if (Py::IsFloat(obj)) {
        consumer->OnDoubleScalar(Py::Float(obj));
    } else if (obj.isNone() || Py_TYPE(obj.ptr()) == reinterpret_cast<PyTypeObject*>(YsonEntityClass)) {
        consumer->OnEntity();
    } else if (HasCallableToYsonType(obj)) {
        auto repr = obj.callMemberFunction("to_yson_type");
        Serialize(repr, consumer, encoding, ignoreInnerAttributes, ysonType, sortKeys, depth, context);
    } else {
        throw CreateYsonError(
            Format(
                "Value %v cannot be serialized to YSON since it has unsupported type %Qv",
                Py::Repr(obj),
                Py::Repr(obj.type())),
            context);
    }
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(Py::Object& obj, INodePtr node, const std::optional<TString>& encoding)
{
    Py::Object attributes = Py::Dict();
    if (!node->Attributes().ListKeys().empty()) {
        Deserialize(attributes, node->Attributes().ToMap(), encoding);
    }

    auto type = node->GetType();
    if (type == ENodeType::Map) {
        auto map = Py::Dict();
        for (auto child : node->AsMap()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child.second, encoding);
            map.setItem(child.first.data(), item);
        }
        obj = NPython::CreateYsonObject("YsonMap", map, attributes);
    } else if (type == ENodeType::Entity) {
        obj = NPython::CreateYsonObject("YsonEntity", Py::None(), attributes);
    } else if (type == ENodeType::Boolean) {
        obj = NPython::CreateYsonObject("YsonBoolean", Py::Boolean(node->AsBoolean()->GetValue()), attributes);
    } else if (type == ENodeType::Int64) {
        obj = NPython::CreateYsonObject("YsonInt64", Py::LongLong(node->AsInt64()->GetValue()), attributes);
    } else if (type == ENodeType::Uint64) {
        obj = NPython::CreateYsonObject("YsonUint64", Py::LongLong(node->AsUint64()->GetValue()), attributes);
    } else if (type == ENodeType::Double) {
        obj = NPython::CreateYsonObject("YsonDouble", Py::Float(node->AsDouble()->GetValue()), attributes);
    } else if (type == ENodeType::String) {
        auto str = Py::Bytes(node->AsString()->GetValue().data());
        if (encoding) {
#if PY_MAJOR_VERSION >= 3
            obj = NPython::CreateYsonObject("YsonUnicode", str.decode(encoding->data()), attributes);
#else
            obj = NPython::CreateYsonObject("YsonString", str.decode(encoding->data()).encode("utf-8"), attributes);
#endif
        } else {
            obj = NPython::CreateYsonObject("YsonString", Py::Bytes(node->AsString()->GetValue().data()), attributes);
        }
    } else if (type == ENodeType::List) {
        auto list = Py::List();
        for (auto child : node->AsList()->GetChildren()) {
            Py::Object item;
            Deserialize(item, child, encoding);
            list.append(item);
        }
        obj = NPython::CreateYsonObject("YsonList", list, attributes);
    } else {
        THROW_ERROR_EXCEPTION("Unsupported node type %Qlv", type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
