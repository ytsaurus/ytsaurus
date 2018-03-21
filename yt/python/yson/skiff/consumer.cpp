#include "consumer.h"
#include "../object_builder.h"

#include <yt/python/yson/serialize.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/misc/finally.h>

namespace NYT {
namespace NPython {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

Py::Object LoadYsonFromStringBuf(const TStringBuf& string)
{
    TPythonObjectBuilder consumer(/* alwaysCreateAttributes */ false, /* encoding */ Null);
    ParseYsonStringBuffer(string, EYsonType::Node, &consumer);
    return consumer.ExtractObject();
}

////////////////////////////////////////////////////////////////////////////////

TPythonSkiffRecordBuilder::TPythonSkiffRecordBuilder(const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& schemas)
    : Schemas_(schemas)
{ }

void TPythonSkiffRecordBuilder::OnBeginRow(ui16 schemaIndex)
{
    if (schemaIndex >= Schemas_.size()) {
        THROW_ERROR_EXCEPTION("Invalid schema index")
            << TErrorAttribute("schema_index", schemaIndex)
            << TErrorAttribute("schema_count", Schemas_.size());
    }

    CurrentSchema_ = Schemas_[schemaIndex];
    CurrentRecord_ = Schemas_[schemaIndex].getCxxObject()->GetSchemaObject()->CreateNewRecord();
}

void TPythonSkiffRecordBuilder::OnEndRow()
{
    Py::Callable classType(TSkiffRecordPython::type());
    Py::PythonClassObject<TSkiffRecordPython> pythonObject(classType.apply(Py::TupleN(CurrentSchema_), Py::Dict()));

    auto cxxObject = pythonObject.getCxxObject();
    cxxObject->SetSkiffRecordObject(CurrentRecord_);
    Objects_.push(pythonObject);
}

void TPythonSkiffRecordBuilder::OnStringScalar(const TStringBuf& value, ui16 columnId)
{
    Py::Bytes field(value.begin(), value.size());
    CurrentRecord_->SetField(columnId, field);
}

void TPythonSkiffRecordBuilder::OnInt64Scalar(i64 value, ui16 columnId)
{
    Py::LongLong field(value);
    CurrentRecord_->SetField(columnId, field);
}

void TPythonSkiffRecordBuilder::OnUint64Scalar(ui64 value, ui16 columnId)
{
    Py::LongLong field(value);
    CurrentRecord_->SetField(columnId, field);
}

void TPythonSkiffRecordBuilder::OnDoubleScalar(double value, ui16 columnId)
{
    Py::Float field(value);
    CurrentRecord_->SetField(columnId, field);
}

void TPythonSkiffRecordBuilder::OnBooleanScalar(bool value, ui16 columnId)
{
    Py::Boolean field(value);
    CurrentRecord_->SetField(columnId, field);
}

void TPythonSkiffRecordBuilder::OnEntity(ui16 columnId)
{
    CurrentRecord_->SetField(columnId, Py::None());
}

void TPythonSkiffRecordBuilder::OnYsonString(const TStringBuf& value, ui16 columnId)
{
    CurrentRecord_->SetField(columnId, LoadYsonFromStringBuf(value));
}

void TPythonSkiffRecordBuilder::OnOtherColumns(const TStringBuf& value)
{
    auto object = LoadYsonFromStringBuf(value);
    auto items = Py::Object(PyDict_Items(*object), true);
    auto iterator = CreateIterator(items);
    while (auto* item = PyIter_Next(*iterator)) {
        auto itemGuard = Finally([item] () { Py::_XDECREF(item); });

        auto key = Py::Object(PyTuple_GetItem(item, 0), false);
        auto value = Py::Object(PyTuple_GetItem(item, 1), false);

        auto mapKey = ConvertStringObjectToString(key);
        CurrentRecord_->SetOtherField(mapKey, value);
    }
}

Py::Object TPythonSkiffRecordBuilder::ExtractObject()
{
    auto object = Objects_.front();
    Objects_.pop();
    return object;
}

bool TPythonSkiffRecordBuilder::HasObject() const
{
    return !Objects_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
