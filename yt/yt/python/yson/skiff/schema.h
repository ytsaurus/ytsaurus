#pragma once

#include "public.h"

#include <yt/yt/library/skiff_ext/public.h>
#include <yt/yt/library/skiff_ext/schema_match.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffSchema
    : public TRefCounted
{
public:
    TSkiffSchema(
        const std::shared_ptr<NSkiff::TSkiffSchema>& skiffSchema,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName);

    virtual ~TSkiffSchema();

    size_t GetDenseFieldsCount();
    size_t GetSparseFieldsCount();

    bool HasOtherColumns();

    size_t Size();

    TIntrusivePtr<TSkiffRecord> CreateNewRecord();

    NSkiffExt::TFieldDescription GetDenseField(ui16 index);
    NSkiffExt::TFieldDescription GetSparseField(ui16 index);
    ui16 GetFieldIndex(const TString& name);
    bool HasField(const TString& name);

    std::shared_ptr<NSkiff::TSkiffSchema> GetSkiffSchema();

private:
    THashMap<TString, ui16> FieldIndices_;
    NSkiffExt::TSkiffTableDescription TableDescription_;

    std::shared_ptr<NSkiff::TSkiffSchema> SkiffSchema_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffSchemaPython
    : public Py::PythonClass<TSkiffSchemaPython>
{
public:
    TSkiffSchemaPython(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    virtual ~TSkiffSchemaPython();

    int sequence_length() override;

    Py::Object CreateRecord();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, CreateRecord)

    Py::Object HasOtherColumns();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, HasOtherColumns)

    Py::Object CopySchema();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, CopySchema)

    Py::Object DeepCopySchema(const Py::Tuple& args);
    PYCXX_VARARGS_METHOD_DECL(TSkiffSchemaPython, DeepCopySchema)

    Py::Object GetFieldNames();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, GetFieldNames)

    static void InitType();

    TIntrusivePtr<TSkiffSchema> GetSchemaObject();

private:
    TIntrusivePtr<TSkiffSchema> Schema_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
