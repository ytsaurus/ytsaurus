#pragma once

#include "helpers.h"
#include "public.h"
#include "stream.h"
#include "object_builder.h"

#include <yt/core/yson/lexer_detail.h>

#include <yt/core/ytree/convert.h>

#include <contrib/libs/pycxx/Extensions.hxx>
#include <contrib/libs/pycxx/Objects.hxx>

#include <Python.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TPyObjectHasher
{
    size_t operator()(const Py::Object& object) const;
};

struct TLazyDictValue
{
    TSharedRefArray Data;
    TNullable<Py::Object> Value;
};

class TLazyDict
{
public:
    typedef THashMap<Py::Object, TLazyDictValue, TPyObjectHasher> THashMapType;

    TLazyDict(bool alwaysCreateAttributes, const TNullable<TString>& encoding);

    PyObject* GetItem(const Py::Object& key);
    void SetItem(const Py::Object& key, const TSharedRefArray& value);
    void SetItem(const Py::Object& key, const Py::Object& value);
    bool HasItem(const Py::Object& key) const;
    void DeleteItem(const Py::Object& key);
    void Clear();
    size_t Length() const;
    THashMapType* GetUnderlyingHashMap();
    Py::Object GetConsumerParams();

private:
    THashMapType Data_;
    std::unique_ptr<NYTree::TPythonObjectBuilder> Consumer_;
    bool AlwaysCreateAttributes_;
    TNullable<TString> Encoding_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
