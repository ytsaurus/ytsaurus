#include "lazy_dict.h"

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/python/yson/pull_object_builder.h>

#include <util/stream/mem.h>
#include <util/generic/typetraits.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

size_t TPyObjectHasher::operator()(const Py::Object& object) const
{
    return object.hashValue();
}

using NPython::GetYsonTypeClass;

TLazyDict::TLazyDict(bool alwaysCreateAttributes, const std::optional<TString>& encoding)
    : YsonInt64(GetYsonTypeClass("YsonInt64"), /* owned */ true)
    , YsonUint64(GetYsonTypeClass("YsonUint64"), /* owned */ true)
    , YsonDouble(GetYsonTypeClass("YsonDouble"), /* owned */ true)
    , YsonBoolean(GetYsonTypeClass("YsonBoolean"), /* owned */ true)
    , YsonEntity(GetYsonTypeClass("YsonEntity"), /* owned */ true)
    , AlwaysCreateAttributes_(alwaysCreateAttributes)
    , Encoding_(encoding)
{
    Tuple1_ = PyObjectPtr(PyTuple_New(1));
    if (!Tuple1_) {
        throw Py::Exception();
    }
}

PyObject* TLazyDict::GetItem(const Py::Object& key)
{
    auto it = Data_.find(key);
    if (it == Data_.end()) {
        Py_RETURN_NONE;
    }
    auto& value = it->second.Value;
    if (value) {
        return value->ptr();
    }
    std::visit([&] (auto&& data) {
            using T = std::decay_t<decltype(data)>;
            if constexpr (std::is_same_v<T, TSharedRef>) {
                auto inputStream = TMemoryInput(data.Begin(), data.Size());
                TYsonPullParser parser(&inputStream, EYsonType::ListFragment);
                NPython::TPullObjectBuilder builder(&parser, AlwaysCreateAttributes_, Encoding_);

                value.emplace(builder.ParseObject().release(), /* owned */ true);
            } else if constexpr (std::is_same_v<T, TYsonItem>) {
                Py::Callable* constructor = nullptr;
                PyObjectPtr result;
                bool isUint = false;

                switch (data.GetType()) {
                    case EYsonItemType::EntityValue:
                        result = PyObjectPtr(Py::new_reference_to(Py_None));
                        constructor = &YsonEntity;
                        break;
                    case EYsonItemType::BooleanValue:
                        result = PyObjectPtr(PyBool_FromLong(data.UncheckedAsBoolean()));
                        constructor = &YsonBoolean;
                        break;
                    case EYsonItemType::Int64Value:
                        result = PyObjectPtr(PyLong_FromLongLong(data.UncheckedAsInt64()));
                        constructor = &YsonInt64;
                        break;
                    case EYsonItemType::Uint64Value:
                        result = PyObjectPtr(PyLong_FromUnsignedLongLong(data.UncheckedAsUint64()));
                        isUint = true;
                        constructor = &YsonUint64;
                        break;
                    case EYsonItemType::DoubleValue:
                        result = PyObjectPtr(PyFloat_FromDouble(data.UncheckedAsDouble()));
                        constructor = &YsonDouble;
                        break;
                    default:
                        YT_ABORT();
                }
                if (!result) {
                    throw Py::Exception();
                }
                if (AlwaysCreateAttributes_ || isUint) {
                    if (PyTuple_SetItem(Tuple1_.get(), 0, result.release()) == -1) {
                        throw Py::Exception();
                    }
                    YT_VERIFY(constructor);
                    result = PyObjectPtr(PyObject_CallObject(constructor->ptr(), Tuple1_.get()));
                    if (!result) {
                        throw Py::Exception();
                    }
                }
                value.emplace(result.release(), /* owned */ true);
            } else {
                static_assert(TDependentFalse<T>, "non-exhaustive visitor!");
            }
        },
        it->second.Data);
    return value->ptr();
}

void TLazyDict::SetItem(const Py::Object& key, const TSharedRef& value)
{
    Data_[key] = {value, std::nullopt};
}

void TLazyDict::SetItem(const Py::Object& key, const Py::Object& value)
{
    Data_[key] = {TSharedRef(), value};
}

void TLazyDict::SetItem(const Py::Object& key, const TYsonItem& value)
{
    Data_[key] = {value, std::nullopt};
}

void TLazyDict::SetItem(const Py::Object& key, const std::variant<TSharedRef, TYsonItem>& value)
{
    Data_[key] = {value, std::nullopt};
}

size_t TLazyDict::Length() const
{
    return Data_.size();
}

bool TLazyDict::HasItem(const Py::Object& key) const
{
    return Data_.find(key) != Data_.end();
}

void TLazyDict::DeleteItem(const Py::Object& key)
{
    Data_.erase(key);
}

void TLazyDict::Clear()
{
    Data_.clear();
}

TLazyDict::THashMapType* TLazyDict::GetUnderlyingHashMap()
{
    return &Data_;
}

Py::Object TLazyDict::GetConsumerParams()
{
    Py::Object encoding;
    if (Encoding_) {
        encoding = Py::String(*Encoding_);
    } else {
        encoding = Py::None();
    }
    return Py::TupleN(encoding, Py::Boolean(AlwaysCreateAttributes_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
