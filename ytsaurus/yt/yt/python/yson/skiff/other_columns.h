#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <library/cpp/skiff/public.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffOtherColumns
    : public Py::PythonClass<TSkiffOtherColumns>
{
public:
    TSkiffOtherColumns(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs);

    int mapping_length() override;
    Py::Object mapping_subscript(const Py::Object& key) override;
    int mapping_ass_subscript(const Py::Object& key, const Py::Object& value) override;
    int sequence_contains(const Py::Object& key) override;
    Py::Object repr() override;

    Py::Object DeepCopy(const Py::Tuple& args);
    PYCXX_VARARGS_METHOD_DECL(TSkiffOtherColumns, DeepCopy)

    static void InitType();

    NYson::TYsonStringBuf GetYsonString();

private:
    std::optional<Py::Bytes> UnparsedBytesObj_;
    std::optional<Py::Mapping> Map_;
    NYson::TYsonString CachedYsonString_;

private:
    void MaybeMaterializeMap();
    TStringBuf GetUnparsedBytes() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
