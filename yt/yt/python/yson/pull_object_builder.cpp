#include "pull_object_builder.h"
#include "yson_lazy_map.h"

#include <yt/yt/core/yson/detail.h>

#include <yt/yt/python/common/helpers.h>

#include <util/stream/str.h>

namespace NYT::NPython {

using namespace NYTree;
using namespace NYson;
using namespace NYson::NDetail;

////////////////////////////////////////////////////////////////////////////////

static constexpr const char* attributesStr = "attributes";

TPullObjectBuilder::TPullObjectBuilder(
    TYsonPullParser* parser,
    bool alwaysCreateAttributes,
    const std::optional<TString>& encoding)
    : Cursor_(parser)
    , YsonMap(GetYsonTypeClass("YsonMap"), /* owned */ true)
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
{
#if PY_MAJOR_VERSION >= 3
    if (auto ysonStringProxyClass = NPython::FindYsonTypeClass("YsonStringProxy")) {
        YsonStringProxy = Py::Callable(ysonStringProxyClass, /* owned */ true);
    }
#endif
    Tuple0_ = PyObjectPtr(PyTuple_New(0));
    if (!Tuple0_) {
        throw Py::Exception();
    }
    Tuple1_ = PyObjectPtr(PyTuple_New(1));
    if (!Tuple1_) {
        throw Py::Exception();
    }

    Py::Object encodingParam;
    if (encoding) {
        encodingParam = Py::String(*encoding);
    } else {
        encodingParam = Py::None();
    }
    LazyMapParserParams_ = Py::TupleN(encodingParam, Py::Boolean(alwaysCreateAttributes));
}

PyObjectPtr TPullObjectBuilder::ParseObject(bool hasAttributes)
{
    auto current = Cursor_.GetCurrent();
    Py::Callable* constructor = nullptr;

    PyObjectPtr result;
    switch (current.GetType()) {
        case EYsonItemType::BeginAttributes: {
            Cursor_.Next();
            auto attributes = ParseMap(EYsonItemType::EndAttributes);
            result = ParseObject(/* hasAttributes */ true);
            if (PyObject_SetAttrString(result.get(), attributesStr, attributes.get()) == -1) {
                throw Py::Exception();
            }
            return result;
        } case EYsonItemType::BeginList: {
            Cursor_.Next();
            return ParseList(hasAttributes);
        } case EYsonItemType::BeginMap: {
            Cursor_.Next();
            return ParseMap(EYsonItemType::EndMap, hasAttributes);
        } case EYsonItemType::EntityValue: {
            result = PyObjectPtr(Py::new_reference_to(Py_None));
            Cursor_.Next();
            constructor = &YsonEntity;
            break;
        } case EYsonItemType::BooleanValue: {
            result = PyObjectPtr(PyBool_FromLong(current.UncheckedAsBoolean()));
            Cursor_.Next();
            constructor = &YsonBoolean;
            break;
        } case EYsonItemType::Int64Value: {
            result = PyObjectPtr(PyLong_FromLongLong(current.UncheckedAsInt64()));
            Cursor_.Next();
            constructor = &YsonInt64;
            break;
        } case EYsonItemType::Uint64Value: {
            result = PyObjectPtr(PyLong_FromUnsignedLongLong(current.UncheckedAsUint64()));
            hasAttributes = true;
            Cursor_.Next();
            constructor = &YsonUint64;
            break;
        } case EYsonItemType::DoubleValue: {
            result = PyObjectPtr(PyFloat_FromDouble(current.UncheckedAsDouble()));
            Cursor_.Next();
            constructor = &YsonDouble;
            break;
        } case EYsonItemType::StringValue: {
            TStringBuf str = current.UncheckedAsString();
            auto bytes = PyObjectPtr(PyBytes_FromStringAndSize(str.data(), str.size()));
            Cursor_.Next();

            if (!bytes) {
                throw Py::Exception();
            }
            if (Encoding_) {
                auto decodedString = PyObjectPtr(
                    PyUnicode_FromEncodedObject(bytes.get(), Encoding_->data(), "strict"));
#if PY_MAJOR_VERSION < 3
                if (!decodedString) {
                    throw Py::Exception();
                }
                result = PyObjectPtr(PyUnicode_AsUTF8String(decodedString.get()));
                constructor = &YsonString;
#else
                if (decodedString) {
                    result = std::move(decodedString);
                    constructor = &YsonUnicode;
                } else {
                    // COMPAT(levysotsky)
                    if (!YsonStringProxy) {
                        throw Py::Exception();
                    }
                    PyErr_Clear();
                    result = PyObjectPtr(PyObject_CallObject(YsonStringProxy->ptr(), Tuple0_.get()));
                    if (!result) {
                        throw Py::Exception();
                    }
                    if (PyObject_SetAttrString(result.get(), "_bytes", bytes.get()) == -1) {
                        throw Py::Exception();
                    }
                    return result;
                }
#endif
            } else {
                result = std::move(bytes);
                constructor = &YsonString;
            }
            break;
        }
        // We don't need to check yson correctness, pull parser does it by itself.
        case EYsonItemType::EndOfStream: {
	        PyErr_SetNone(PyExc_StopIteration);
            return nullptr;
	    }
        case EYsonItemType::EndAttributes:
        case EYsonItemType::EndMap:
        case EYsonItemType::EndList: {
            // Bad yson, as we skip these tokens in ParseList or ParseMap.
            // Pull parser checks yson correctness, so this code can't be reached.
            YT_ABORT();
        }
    }
    if (!result) {
        throw Py::Exception();
    }
    if (hasAttributes || AlwaysCreateAttributes_) {
        if (PyTuple_SetItem(Tuple1_.get(), 0, result.release()) == -1) {
            throw Py::Exception();
        }
        Y_VERIFY(constructor);
        result = PyObjectPtr(PyObject_CallObject(constructor->ptr(), Tuple1_.get()));
        if (!result) {
            throw Py::Exception();
        }
    }
    return result;
}

PyObjectPtr TPullObjectBuilder::ParseList(bool hasAttributes)
{
    PyObjectPtr listObj;
    if (hasAttributes || AlwaysCreateAttributes_) {
        listObj = PyObjectPtr(PyObject_CallObject(YsonList.ptr(), Tuple0_.get()));
    } else {
        listObj = PyObjectPtr(PyList_New(0));
    }

    if (!listObj) {
        throw Py::Exception();
    }

    while (Cursor_.GetCurrent().GetType() != EYsonItemType::EndList) {
        auto value = ParseObject();
        if (PyList_Append(listObj.get(), value.get()) == -1) {
            throw Py::Exception();
        }
    }
    Cursor_.Next(); // skip end of list

    return listObj;
}

PyObjectPtr TPullObjectBuilder::ParseMap(EYsonItemType endType, bool hasAttributes)
{
    PyObjectPtr dictObj;

    // If endType == EndAttributes, we are reading attributes, so we should not wrap them to YsonMap.
    if ((hasAttributes || AlwaysCreateAttributes_) && endType != EYsonItemType::EndAttributes) {
        dictObj = PyObjectPtr(PyObject_CallObject(YsonMap.ptr(), Tuple0_.get()));
    } else {
        dictObj = PyObjectPtr(PyDict_New());
    }

    if (!dictObj) {
        throw Py::Exception();
    }

    while (Cursor_.GetCurrent().GetType() != endType) {
        auto keyStr = Cursor_.GetCurrent().UncheckedAsString();
        auto key = KeyCache_.GetPythonString(keyStr);
        Cursor_.Next();
        auto value = ParseObject();
        if (PyDict_SetItem(dictObj.get(), key.get(), value.get()) == -1) {
            throw Py::Exception();
        }
    }
    Cursor_.Next(); // skip end token

    return dictObj;
}

PyObjectPtr TPullObjectBuilder::ParseObjectLazy(bool hasAttributes)
{
    auto current = Cursor_.GetCurrent();
    switch (current.GetType()) {
        case EYsonItemType::BeginAttributes: {
            Cursor_.Next();
            auto attributes = ParseMapLazy(EYsonItemType::EndAttributes);
            auto result = ParseObjectLazy(/* hasAttributes */ true);
            if (PyObject_SetAttrString(result.get(), attributesStr, attributes.get()) == -1) {
                throw Py::Exception();
            }
            return result;
        }
        case EYsonItemType::BeginList:
            Cursor_.Next();
            return ParseList();
        case EYsonItemType::BeginMap:
            Cursor_.Next();
            return ParseMapLazy(EYsonItemType::EndMap);
        default:
            return ParseObject(hasAttributes);
    }
}

PyObjectPtr TPullObjectBuilder::ParseMapLazy(EYsonItemType endType)
{
    PyObjectPtr dictObj;
    TLazyDict* lazyDict;
    // TODO(egor-gutrov): refactor
    if (endType == EYsonItemType::EndAttributes) {
        dictObj = PyObjectPtr(LazyYsonMapBaseNew(TLazyYsonMapBaseType, Py_None, Py_None));
        LazyYsonMapBaseInit(reinterpret_cast<TLazyYsonMapBase*>(dictObj.get()), LazyMapParserParams_.ptr(), Py::Dict().ptr());
        TLazyYsonMapBase* object = reinterpret_cast<TLazyYsonMapBase*>(dictObj.get());
        lazyDict = object->Dict;
    } else {
        dictObj = PyObjectPtr(LazyYsonMapNew(TLazyYsonMapType, Py_None, Py_None));
        LazyYsonMapInit(reinterpret_cast<TLazyYsonMap*>(dictObj.get()), LazyMapParserParams_.ptr(), Py::Dict().ptr());
        TLazyYsonMap* object = reinterpret_cast<TLazyYsonMap*>(dictObj.get());
        lazyDict = object->super.Dict;
    }

    TStringStream data;
    while (Cursor_.GetCurrent().GetType() != endType) {
        auto keyStr = Cursor_.GetCurrent().UncheckedAsString();
        auto key = KeyCache_.GetPythonString(keyStr);

        Cursor_.StartRecording(&data);
        Cursor_.Next();
        switch (Cursor_.GetCurrent().GetType()) {
            case EYsonItemType::EntityValue:
            case EYsonItemType::BooleanValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::DoubleValue:
                Cursor_.CancelRecording();
                data.Clear();
                lazyDict->SetItem(Py::Object(key.get()), Cursor_.GetCurrent());
                Cursor_.Next();
                break;
            default: {
                Cursor_.SkipComplexValueAndFinishRecording();
                auto value = TSharedRef::FromString(data.Str());
                data = TStringStream();
                // value contains KeyValueSeparatorSymbol and an arbitrary amount of spaces before it
                int separatorPos = -1;
                for (size_t index = 0; index < value.Size(); ++index) {
                    if (value[index] == KeyValueSeparatorSymbol) {
                        separatorPos = index;
                        break;
                    }
                }
                YT_VERIFY(separatorPos != -1);
                value = value.Slice(separatorPos + 1, value.Size());
                lazyDict->SetItem(Py::Object(key.get()), value);
            }
        }
    }
    Cursor_.Next(); // skip end token

    return dictObj;
 }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
