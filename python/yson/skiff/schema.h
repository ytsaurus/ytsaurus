#pragma once

#include "public.h"

#include <yt/core/skiff/public.h>
#include <yt/core/skiff/schema_match.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffSchema
    : public TIntrinsicRefCounted
{
public:
    TSkiffSchema(
        const NSkiff::TSkiffSchemaPtr& skiffSchema,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName);

    virtual ~TSkiffSchema();

    size_t GetDenseFieldsCount();
    size_t GetSparseFieldsCount();

    bool HasOtherColumns();

    size_t Size();

    TIntrusivePtr<TSkiffRecord> CreateNewRecord();

    NSkiff::TDenseFieldDescription GetDenceField(ui16 index);
    NSkiff::TSparseFieldDescription GetSparseField(ui16 index);
    ui16 GetFieldIndex(const TString& name);
    bool HasField(const TString& name);

    NSkiff::TSkiffSchemaPtr GetSkiffSchema();

private:
    THashMap<TString, ui16> FieldIndeces_;
    NSkiff::TSkiffTableDescription TableDescription_;

    NSkiff::TSkiffSchemaPtr SkiffSchema_;
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
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, CreateRecord);

    Py::Object HasOtherColumns();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, HasOtherColumns);

    Py::Object CopySchema();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, CopySchema);

    Py::Object DeepCopySchema(const Py::Tuple& args);
    PYCXX_VARARGS_METHOD_DECL(TSkiffSchemaPython, DeepCopySchema);

    Py::Object GetFieldNames();
    PYCXX_NOARGS_METHOD_DECL(TSkiffSchemaPython, GetFieldNames);

    static void InitType();

    TIntrusivePtr<TSkiffSchema> GetSchemaObject();

private:
    TIntrusivePtr<TSkiffSchema> Schema_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
