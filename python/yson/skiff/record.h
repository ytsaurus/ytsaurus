#pragma once

#include "public.h"

#include <yt/core/misc/ref_counted.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffRecord
    : public TIntrinsicRefCounted
{
public:
    explicit TSkiffRecord(TSkiffSchema* schema);
    TSkiffRecord(
        TIntrusivePtr<TSkiffSchema> schema,
        const std::vector<Py::Object>& denseFields,
        const THashMap<ui16, Py::Object>& sparseFields,
        const THashMap<TString, Py::Object>& otherFields);

    ~TSkiffRecord();

    Py::Object GetField(ui16 index);
    Py::Object GetField(const TString& key);

    void SetField(ui16 index, const Py::Object& value);
    void SetField(const TString& key, const Py::Object& value);

    Py::Object GetDenseField(ui16 index);
    void SetDenceField(ui16 index, const Py::Object& value);

    Py::Object GetSparseField(ui16 index);
    void SetSparseField(ui16 index, const Py::Object& value);

    void SetOtherField(const TString& key, const Py::Object& value);

    THashMap<TString, Py::Object>* GetOtherFields();
    THashMap<ui16, Py::Object>* GetSparseFields();

    TIntrusivePtr<TSkiffRecord> DeepCopy();

    size_t Size();

    TIntrusivePtr<TSkiffSchema> GetSchema();

private:
    TIntrusivePtr<TSkiffSchema> Schema_;

    std::vector<Py::Object> DenseFields_;
    THashMap<ui16, Py::Object> SparseFields_;
    THashMap<TString, Py::Object> OtherFields_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffRecordPython
    : public Py::PythonClass<TSkiffRecordPython>
{
public:
    TSkiffRecordPython(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    virtual ~TSkiffRecordPython();

    Py::Object mapping_subscript(const Py::Object& key) override;
    int mapping_ass_subscript(const Py::Object& key, const Py::Object& value) override;
    PyCxx_ssize_t mapping_length() override;

    Py::Object iter();

    Py::Object GetItemsIterator();
    PYCXX_NOARGS_METHOD_DECL(TSkiffRecordPython, GetItemsIterator);

    Py::Object GetSchema();
    PYCXX_NOARGS_METHOD_DECL(TSkiffRecordPython, GetSchema);

    Py::Object CopyRecord();
    PYCXX_NOARGS_METHOD_DECL(TSkiffRecordPython, CopyRecord);

    Py::Object DeepCopyRecord(const Py::Tuple& args);
    PYCXX_VARARGS_METHOD_DECL(TSkiffRecordPython, DeepCopyRecord);

    void SetSkiffRecordObject(TIntrusivePtr<TSkiffRecord> record);

    static void InitType();

    TIntrusivePtr<TSkiffRecord> GetRecordObject();

private:
    TIntrusivePtr<TSkiffRecord> Record_;
    Py::Object Schema_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffRecordItemsIterator
    : public Py::PythonClass<TSkiffRecordItemsIterator>
{
public:
    TSkiffRecordItemsIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    virtual ~TSkiffRecordItemsIterator();

    void Init(const TIntrusivePtr<TSkiffRecord>& record);

    Py::Object iter();

    PyObject* iternext();

    static void InitType();

private:
    TIntrusivePtr<TSkiffRecord> Record_;

    ui16 NextDenseFieldIndex_;
    THashMap<ui16, Py::Object>::const_iterator SparseFieldsIterator_;
    THashMap<TString, Py::Object>::const_iterator OtherFieldsIterator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
