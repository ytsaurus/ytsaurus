#pragma once

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/public.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/core/yson/lexer_detail.h>

#include <yt/yt/core/ytree/convert.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <Python.h>

namespace NYT::NYTree {

using NPython::PyObjectPtr;

////////////////////////////////////////////////////////////////////////////////

struct TPyObjectHasher
{
    size_t operator()(const Py::Object& object) const;
};

struct TLazyDictValue
{
    // For simple types we store TYsonItem.
    // For strings and complex objects we store TSharedRef.
    std::variant<TSharedRef, NYson::TYsonItem> Data;
    std::optional<Py::Object> Value;
};

class TLazyDict
{
public:
    typedef THashMap<Py::Object, TLazyDictValue, TPyObjectHasher> THashMapType;

    TLazyDict(bool alwaysCreateAttributes, const std::optional<TString>& encoding);

    PyObject* GetItem(const Py::Object& key);
    void SetItem(const Py::Object& key, const TSharedRef& value);
    void SetItem(const Py::Object& key, const Py::Object& value);
    void SetItem(const Py::Object& key, const NYson::TYsonItem& value);
    void SetItem(const Py::Object& key, const std::variant<TSharedRef, NYson::TYsonItem>& value);

    bool HasItem(const Py::Object& key) const;
    void DeleteItem(const Py::Object& key);
    void Clear();
    size_t Length() const;
    THashMapType* GetUnderlyingHashMap();
    Py::Object GetConsumerParams();

private:
    THashMapType Data_;

    Py::Callable YsonInt64;
    Py::Callable YsonUint64;
    Py::Callable YsonDouble;
    Py::Callable YsonBoolean;
    Py::Callable YsonEntity;
    PyObjectPtr Tuple1_;
    
    bool AlwaysCreateAttributes_;
    std::optional<TString> Encoding_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
