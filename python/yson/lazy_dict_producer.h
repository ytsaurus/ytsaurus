#pragma once

#include "lazy_dict.h"
#include "object_builder.h"
#include "yson_lazy_map.h"

#include <Objects.hxx> // pycxx

#include <Python.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TLazyDictProducer
{
public:
    TLazyDictProducer();
    TLazyDictProducer(const std::optional<TString>& encoding, bool alwaysCreateAttributes);

    Py::Object ExtractObject();

    void SetObject();
    void OnKeyValue(const Py::Object& key, const TSharedRef& value);

    void OnBeginAttributes();
    void OnEndAttributes();

    NYTree::TPythonObjectBuilder* GetPythonObjectBuilder();

private:
    void Reset();

    Py::Object ParserParams_;

    NYTree::TLazyDict* LazyDict_;
    NYTree::TLazyDict* LazyAttributesDict_;
    Py::Object ResultObject_;

    bool InsideAttributes_ = false;

    NYTree::TPythonObjectBuilder PythonObjectBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
