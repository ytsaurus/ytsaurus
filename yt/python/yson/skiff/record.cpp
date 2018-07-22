#include "record.h"
#include "schema.h"

#include <yt/python/common/helpers.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

TSkiffRecord::TSkiffRecord(TSkiffSchema* schema)
    : Schema_(schema)
{
    DenseFields_.assign(Schema_->GetDenseFieldsCount(), Py::None());
}

TSkiffRecord::TSkiffRecord(
    TIntrusivePtr<TSkiffSchema> schema,
    const std::vector<Py::Object>& denseFields,
    const THashMap<ui16, Py::Object>& sparseFields,
    const THashMap<TString, Py::Object>& otherFields)
    : Schema_(schema)
    , DenseFields_(denseFields)
    , SparseFields_(sparseFields)
    , OtherFields_(otherFields)
{ }

TSkiffRecord::~TSkiffRecord()
{ }

Py::Object TSkiffRecord::GetField(ui16 index)
{
    if (index < Schema_->GetDenseFieldsCount()) {
        return GetDenseField(index);
    } else if (index < Schema_->Size()) {
        return GetSparseField(index);
    }
    throw Py::IndexError(ToString(index));
}

Py::Object TSkiffRecord::GetField(const TString& key)
{
    if (Schema_->HasField(key)) {
        auto fieldIndex = Schema_->GetFieldIndex(key);
        if (fieldIndex < Schema_->GetDenseFieldsCount()) {
            return GetDenseField(fieldIndex);
        }
        return GetSparseField(fieldIndex);
    }
    auto it = OtherFields_.find(key);
    if (it != OtherFields_.end()) {
        return it->second;
    }
    throw Py::KeyError(key);
}

void TSkiffRecord::SetField(ui16 index, const Py::Object& value)
{
    if (index < Schema_->GetDenseFieldsCount()) {
        SetDenceField(index, value);
    } else if (index < Schema_->Size()) {
        SetSparseField(index, value);
    } else {
        throw Py::IndexError(ToString(index));
    }
}

void TSkiffRecord::SetField(const TString& key, const Py::Object& value)
{
    if (Schema_->HasField(key)) {
        auto fieldIndex = Schema_->GetFieldIndex(key);
        SetField(fieldIndex, value);
        return;
    }

    if (!Schema_->HasOtherColumns()) {
        throw Py::KeyError(key);
    }
    SetOtherField(key, value);
}

Py::Object TSkiffRecord::GetDenseField(ui16 index)
{
    if (index >= Schema_->GetDenseFieldsCount()) {
        throw Py::IndexError(ToString(index));
    }
    return DenseFields_[index];
}

Py::Object TSkiffRecord::GetSparseField(ui16 index)
{
    const auto it = SparseFields_.find(index);
    if (it == SparseFields_.end()) {
        return Py::None();
    }
    return it->second;
}


void CheckFieldType(const Py::Object& value, NSkiff::TSkiffSchemaPtr schema, bool required)
{
    thread_local PyObject* Zero = PyLong_FromLongLong(0);
    thread_local PyObject* SignedInt64Min = PyLong_FromLongLong(std::numeric_limits<i64>::min());
    thread_local PyObject* SignedInt64Max = PyLong_FromLongLong(std::numeric_limits<i64>::max());
    thread_local PyObject* UnsignedInt64Max = PyLong_FromUnsignedLongLong(std::numeric_limits<ui64>::max());;

    if (value.isNone()) {
        if (required) {
            THROW_ERROR_EXCEPTION("Field is required, but None passed to SkiffRecord");
        }
        return;
    }

    switch (schema->GetWireType()) {
        case NSkiff::EWireType::Int64:
        case NSkiff::EWireType::Uint64: {
            auto valueAsLongLong = Py::LongLong(value);
            if (schema->GetWireType() == NSkiff::EWireType::Uint64 &&
                (
                    PyObject_RichCompareBool(Zero, valueAsLongLong.ptr(), Py_LE) != 1 ||
                    PyObject_RichCompareBool(valueAsLongLong.ptr(), UnsignedInt64Max, Py_LT) != 1
                )
            )
            {
                THROW_ERROR_EXCEPTION("Invalid value passed to SkiffRecord, it must be in range [0, 2^64 - 1]")
                    << TErrorAttribute("expected_type", "uint64")
                    << TErrorAttribute("value", ConvertStringObjectToString(valueAsLongLong.str()));
            }
            if (schema->GetWireType() == NSkiff::EWireType::Int64 &&
                (
                    PyObject_RichCompareBool(SignedInt64Min, valueAsLongLong.ptr(), Py_LE) != 1 ||
                    PyObject_RichCompareBool(valueAsLongLong.ptr(), SignedInt64Max, Py_LE) != 1
                )
            )
            {
                THROW_ERROR_EXCEPTION("Invalid value passed to SkiffRecord, it must be in range [0, 2^64 - 1]")
                    << TErrorAttribute("expected_type", "int64")
                    << TErrorAttribute("value", ConvertStringObjectToString(valueAsLongLong.str()));
            }
            break;
        }
        case NSkiff::EWireType::Boolean: {
            if (!value.isBoolean()) {
                THROW_ERROR_EXCEPTION("Invalid value passed to SkiffRecord")
                    << TErrorAttribute("expected_type", "boolean")
                    << TErrorAttribute("value", TString(value.as_string()));
            }
            break;
        }
        case NSkiff::EWireType::Double: {
            Py::Float(value);
            break;
        }
        case NSkiff::EWireType::String32: {
            if (!(PyBytes_Check(value.ptr()) || PyUnicode_Check(value.ptr()))) {
                THROW_ERROR_EXCEPTION("Invalid value passed to SkiffRecord")
                    << TErrorAttribute("expected_type", "string")
                    << TErrorAttribute("value", TString(value.as_string()));
            }
            break;
        }
        case NSkiff::EWireType::Yson32: {
            break;
        }
        default:
            Y_UNREACHABLE();
    }
}


void TSkiffRecord::SetDenceField(ui16 index, const Py::Object& value)
{
    auto newFieldValue = value;
    if (newFieldValue.ptr() == nullptr) {
        newFieldValue = Py::None();
    }
    auto fieldDescription = Schema_->GetDenceField(index);
    CheckFieldType(newFieldValue, fieldDescription.DeoptionalizedSchema, fieldDescription.Required);
    DenseFields_[index] = newFieldValue;
}

void TSkiffRecord::SetSparseField(ui16 index, const Py::Object& value)
{
    if (value.ptr() == nullptr) {
        SparseFields_.erase(index);
        return;
    }
    auto fieldDescription = Schema_->GetSparseField(index - Schema_->GetDenseFieldsCount());
    CheckFieldType(value, fieldDescription.DeoptionalizedSchema, false);
    SparseFields_[index] = value;
}

void TSkiffRecord::SetOtherField(const TString& key, const Py::Object& value)
{
    if (value.ptr() == nullptr) {
        OtherFields_.erase(key);
    } else {
        OtherFields_[key] = value;
    }
}

size_t TSkiffRecord::Size()
{
    return DenseFields_.size() + SparseFields_.size() + OtherFields_.size();
}

THashMap<TString, Py::Object>* TSkiffRecord::GetOtherFields()
{
    return &OtherFields_;
}

THashMap<ui16, Py::Object>* TSkiffRecord::GetSparseFields()
{
    return &SparseFields_;
};

TIntrusivePtr<TSkiffRecord> TSkiffRecord::DeepCopy()
{
    static Py::Callable deepcopyFunction;
    if (deepcopyFunction.isNone()) {
        auto ptr = PyImport_ImportModule("copy");
        if (!ptr) {
            throw Py::RuntimeError("Failed to import module copy");
        }
        auto module = Py::Object(ptr);
        deepcopyFunction = Py::Callable(Py::GetAttr(module, "deepcopy"));
        deepcopyFunction.increment_reference_count();
    }

    std::vector<Py::Object> denseFields;
    THashMap<ui16, Py::Object> sparseFields;
    THashMap<TString, Py::Object> otherFields;

    for (const auto& field : DenseFields_) {
        denseFields.push_back(deepcopyFunction.apply(Py::TupleN(field)));
    }

    for (const auto& field : SparseFields_) {
        sparseFields[field.first] = deepcopyFunction.apply(Py::TupleN(field.second));
    }

    for (const auto& field : OtherFields_) {
        otherFields[field.first] = deepcopyFunction.apply(Py::TupleN(field.second));
    }

    TIntrusivePtr<TSkiffRecord> result = New<TSkiffRecord>(
        Schema_,
        denseFields,
        sparseFields,
        otherFields);
    return result;
}

TIntrusivePtr<TSkiffSchema> TSkiffRecord::GetSchema()
{
    return Schema_;
}

////////////////////////////////////////////////////////////////////////////////

TSkiffRecordPython::TSkiffRecordPython(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TSkiffRecordPython>::PythonClass(self, args, kwargs)
{
    Schema_ = ExtractArgument(args, kwargs, "schema");
    ValidateArgumentsEmpty(args, kwargs);
}

TSkiffRecordPython::~TSkiffRecordPython() = default;

Py::Object TSkiffRecordPython::mapping_subscript(const Py::Object& key)
{
    if (Py::IsInteger(key)) {
        auto index = Py::ConvertToLongLong(key);
        return Record_->GetField(index);
    }
    auto fieldName = Py::ConvertStringObjectToString(key);
    return Record_->GetField(fieldName);
}

int TSkiffRecordPython::mapping_ass_subscript(const Py::Object& key, const Py::Object& value)
{
    if (Py::IsInteger(key)) {
        auto index = Py::ConvertToLongLong(key);
        Record_->SetField(index, value);
    } else {
        auto fieldName = Py::ConvertStringObjectToString(key);
        Record_->SetField(fieldName, value);
    }
    return 0;
}

PyCxx_ssize_t TSkiffRecordPython::mapping_length()
{
    return Record_->Size();
}

void TSkiffRecordPython::SetSkiffRecordObject(TIntrusivePtr<TSkiffRecord> record)
{
    Record_ = record;
}

void TSkiffRecordPython::InitType()
{
    behaviors().name("SkiffRecord");
    behaviors().doc("Skiff record");

    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    behaviors().supportMappingType(
        behaviors().support_mapping_ass_subscript |
        behaviors().support_mapping_subscript |
        behaviors().support_mapping_length);

    PYCXX_ADD_NOARGS_METHOD(get_schema, GetSchema, "Return schema associated with record");
    PYCXX_ADD_NOARGS_METHOD(__copy__, CopyRecord, "Make shallow copy");
    PYCXX_ADD_NOARGS_METHOD(items, GetItemsIterator, "Return iterator over record items");
    PYCXX_ADD_VARARGS_METHOD(__deepcopy__, DeepCopyRecord, "Make deep copy");

    behaviors().readyType();
}

TIntrusivePtr<TSkiffRecord> TSkiffRecordPython::GetRecordObject()
{
    return Record_;
}

Py::Object TSkiffRecordPython::GetSchema()
{
    return Schema_;
}

Py::Object TSkiffRecordPython::CopyRecord()
{
    Py::Callable classType(TSkiffRecordPython::type());
    Py::PythonClassObject<TSkiffRecordPython> result(classType.apply(Py::TupleN(Schema_), Py::Dict()));
    result.getCxxObject()->SetSkiffRecordObject(Record_);
    return result;
}

Py::Object TSkiffRecordPython::DeepCopyRecord(const Py::Tuple& args)
{
    Py::Callable classType(TSkiffRecordPython::type());
    Py::PythonClassObject<TSkiffSchemaPython> schemaObject(Schema_);
    auto schema = schemaObject.getCxxObject()->DeepCopySchema(Py::Tuple());
    Py::PythonClassObject<TSkiffRecordPython> result(classType.apply(Py::TupleN(schema), Py::Dict()));
    TIntrusivePtr<TSkiffRecord> newRecord = Record_->DeepCopy();
    result.getCxxObject()->SetSkiffRecordObject(newRecord);
    return result;
}

Py::Object TSkiffRecordPython::iter()
{
    return GetItemsIterator();
}

Py::Object TSkiffRecordPython::GetItemsIterator()
{
    Py::Callable classType(TSkiffRecordItemsIterator::type());
    Py::PythonClassObject<TSkiffRecordItemsIterator> result(classType.apply(Py::Tuple(), Py::Dict()));

    result.getCxxObject()->Init(Record_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TSkiffRecordItemsIterator::TSkiffRecordItemsIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TSkiffRecordItemsIterator>::PythonClass(self, args, kwargs)
{ }

TSkiffRecordItemsIterator::~TSkiffRecordItemsIterator() = default;

void TSkiffRecordItemsIterator::Init(const TIntrusivePtr<TSkiffRecord>& record)
{
    Record_ = record;
    NextDenseFieldIndex_ = 0;
    SparseFieldsIterator_ = Record_->GetSparseFields()->begin();
    OtherFieldsIterator_ = Record_->GetOtherFields()->begin();
}

Py::Object TSkiffRecordItemsIterator::iter()
{
    return self();
}

PyObject* TSkiffRecordItemsIterator::iternext()
{
    auto schema = Record_->GetSchema();
    while (NextDenseFieldIndex_ < schema->GetDenseFieldsCount()) {
        auto key = Py::String(schema->GetDenceField(NextDenseFieldIndex_).Name);
        auto value = Record_->GetDenseField(NextDenseFieldIndex_);
        ++NextDenseFieldIndex_;

        if (value.isNone()) {
            continue;
        }

        auto result = Py::TupleN(key, value);
        result.increment_reference_count();
        return result.ptr();
    }

    if (SparseFieldsIterator_ != Record_->GetSparseFields()->end()) {
        auto key = Py::String(schema->GetSparseField(SparseFieldsIterator_->first - schema->GetDenseFieldsCount()).Name);
        const auto& value = SparseFieldsIterator_->second;
        ++SparseFieldsIterator_;

        auto result = Py::TupleN(key, value);
        result.increment_reference_count();
        return result.ptr();
    }

    if (OtherFieldsIterator_ != Record_->GetOtherFields()->end()) {
        auto key = Py::String(OtherFieldsIterator_->first);
        const auto& value = OtherFieldsIterator_->second;
        ++OtherFieldsIterator_;

        auto result = Py::TupleN(key, value);
        result.increment_reference_count();
        return result.ptr();
    }

    PyErr_SetNone(PyExc_StopIteration);
    return 0;
}

void TSkiffRecordItemsIterator::InitType()
{
    behaviors().name("Skiff record iterator");
    behaviors().doc("Iterates over Skiff record");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
