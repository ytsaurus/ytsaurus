#include "serialize.h"

#include "converter_python_to_skiff.h"
#include "error.h"
#include "schema.h"
#include "record.h"
#include "switch.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/python/yson/helpers.h>
#include <yt/yt/python/yson/serialize.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/skiff/skiff.h>

namespace NYT::NPython {

using namespace NSkiff;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SerializeField(
    NSkiff::EWireType wireType,
    const Py::Object& object,
    bool required,
    const std::optional<TString>& encoding,
    NSkiff::TCheckedInDebugSkiffWriter* skiffWriter)
{
    if (!required) {
        if (object.isNone()) {
            skiffWriter->WriteVariant8Tag(0);
            return;
        } else {
            skiffWriter->WriteVariant8Tag(1);
        }
    }
    switch (wireType) {
        case NSkiff::EWireType::Int64: {
            auto value = PyLong_AsLongLong(object.ptr());
            if (PyErr_Occurred()) {
                throw Py::Exception();
            }
            skiffWriter->WriteInt64(value);
            break;
        }
        case NSkiff::EWireType::Uint64: {
            auto value = PyLong_AsUnsignedLongLong(object.ptr());
            if (PyErr_Occurred()) {
                throw Py::Exception();
            }
            skiffWriter->WriteUint64(value);
            break;
        }
        case NSkiff::EWireType::Boolean: {
            auto value = Py::Boolean(object);
            skiffWriter->WriteBoolean(value);
            break;
        }
        case NSkiff::EWireType::Double: {
            auto value = Py::Float(object);
            skiffWriter->WriteDouble(value);
            break;
        }
        case NSkiff::EWireType::String32: {
            auto encodedString = EncodeStringObject(object, encoding);
            auto value = ConvertToStringBuf(encodedString);
            skiffWriter->WriteString32(value);
            break;
        }
        case NSkiff::EWireType::Yson32: {
            auto value = ConvertToYsonString(object, EYsonFormat::Text);
            skiffWriter->WriteYson32(value.AsStringBuf());
            break;
        }
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void SerializeSkiff(
    const TIntrusivePtr<TSkiffRecord>& record,
    NSkiff::TCheckedInDebugSkiffWriter* skiffWriter,
    const TIntrusivePtr<TSkiffSchema>& schema,
    const std::optional<TString>& encoding)
{
    for (ui16 idx = 0; idx < schema->GetDenseFieldsCount(); ++idx) {
        const auto& fieldInfo = schema->GetDenseField(idx);
        const auto& object = record->GetDenseField(idx);

        SerializeField(fieldInfo.ValidatedSimplify(), object, fieldInfo.IsRequired(), encoding, skiffWriter);
    }

    if (schema->GetSparseFieldsCount() > 0) {
        for (ui16 idx = 0; idx < schema->GetSparseFieldsCount(); ++idx) {
            const auto& fieldInfo = schema->GetSparseField(idx);
            const auto& object = record->GetSparseField(idx + schema->GetDenseFieldsCount());

            if (object.isNone()) {
                continue;
            }

            skiffWriter->WriteVariant16Tag(idx);
            SerializeField(fieldInfo.ValidatedSimplify(), object, /* required */ true, encoding, skiffWriter);
        }
        skiffWriter->WriteVariant16Tag(NSkiff::EndOfSequenceTag<ui16>());
    }

    if (schema->HasOtherColumns()) {
        TString result;
        TStringOutput stringOutput(result);

        auto writer = CreateYsonWriter(
            &stringOutput,
            EYsonFormat::Binary,
            EYsonType::Node,
            /* enableRaw */ false);

        NYTree::BuildYsonFluently(writer.get())
            .DoMapFor(*record->GetOtherFields(), [=] (NYTree::TFluentMap fluent, const std::pair<TString, Py::Object>& column) {
                fluent
                    .Item(column.first)
                    .Value(column.second);
            });

        writer->Flush();

        skiffWriter->WriteYson32(result);
    }
}

////////////////////////////////////////////////////////////////////////////////

Py::Object DumpSkiff(Py::Tuple& args, Py::Dict& kwargs)
{
    auto objects = ExtractArgument(args, kwargs, "objects");
    auto streamsArg = ExtractArgument(args, kwargs, "streams");
    if (!streamsArg.isList()) {
        throw Py::TypeError("\"streams\" should be a list");
    }
    auto streams = Py::List(streamsArg);

    std::vector<std::unique_ptr<IZeroCopyOutput>> outputStreams;
    for (const auto& stream : streams) {
        outputStreams.push_back(CreateZeroCopyOutputStreamWrapper(stream));
    }

    auto schemasArg = ExtractArgument(args, kwargs, "schemas");
    if (!schemasArg.isList()) {
        throw Py::TypeError("\"schemas\" should be a list");
    }
    auto schemas = Py::List(schemasArg);

    std::optional<TString> encoding("utf-8");
    if (HasArgument(args, kwargs, "encoding")) {
        auto arg = ExtractArgument(args, kwargs, "encoding");
        if (arg.isNone()) {
            encoding = std::nullopt;
        } else {
            encoding = ConvertStringObjectToString(arg);
        }
    }

    ValidateArgumentsEmpty(args, kwargs);

    std::vector<TIntrusivePtr<TSkiffSchema>> schemaObjects;
    std::vector<std::unique_ptr<NSkiff::TCheckedInDebugSkiffWriter>> skiffWriters;
    for (ssize_t index = 0; index < streams.size(); ++index) {
        auto schema = schemas[index];
        Py::PythonClassObject<TSkiffSchemaPython> schemaPythonObject(schema);
        auto cxxObject = schemaPythonObject.getCxxObject();
        auto schemaObject = cxxObject->GetSchemaObject();
        schemaObjects.push_back(schemaObject);

        NSkiff::TSkiffSchemaList schemaList;
        schemaList.push_back(schemaObject->GetSkiffSchema());
        std::unique_ptr<NSkiff::TCheckedInDebugSkiffWriter> skiffWriter(
            new NSkiff::TCheckedInDebugSkiffWriter(CreateVariant16Schema(schemaList), outputStreams[index].get()));
        skiffWriters.push_back(std::move(skiffWriter));
    }

    ui16 tableIndex = 0;

    auto iterator = CreateIterator(objects);
    while (auto* item_ = PyIter_Next(*iterator)) {
        Py::Object item(item_, /* owned */ true);
        if (TSkiffTableSwitchPython::check(item)) {
            Py::PythonClassObject<TSkiffTableSwitchPython> switcher(item);
            tableIndex = switcher.getCxxObject()->GetTableIndex();
            if (tableIndex >= schemaObjects.size()) {
                THROW_ERROR_EXCEPTION("Invalid table index")
                    << TErrorAttribute("table_index", tableIndex);
            }
            continue;
        }

        skiffWriters[tableIndex]->WriteVariant16Tag(0);
        Py::PythonClassObject<TSkiffRecordPython> record(item.ptr());
        auto cxxObject = record.getCxxObject();
        auto recordObject = cxxObject->GetRecordObject();
        SerializeSkiff(recordObject, skiffWriters[tableIndex].get(), schemaObjects[tableIndex], encoding);
    }
    if (PyErr_Occurred()) {
        throw Py::Exception();
    }

    for (const auto& skiffWriter : skiffWriters) {
        skiffWriter->Finish();
    }
    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

static bool IsOutputRow(const Py::Object& obj)
{
    static PyObjectPtr RowClass(GetModuleAttribute("yt.wrapper.schema", "OutputRow"));
    return Py_TYPE(obj.ptr()) == reinterpret_cast<PyTypeObject*>(RowClass.get());
}

static bool IsYtDataclass(const Py::Object& obj)
{
    return obj.hasAttr("_YT_DATACLASS_MARKER");
}

static Py::Object DumpSkiffStructuredImpl(Py::Tuple &args, Py::Dict &kwargs)
{
    auto objects = ExtractArgument(args, kwargs, "objects");
    auto streamsArg = ExtractArgument(args, kwargs, "streams");
    if (!streamsArg.isList()) {
        throw Py::TypeError("\"streams\" should be a list");
    }
    auto streams = Py::List(streamsArg);

    std::vector<std::unique_ptr<IZeroCopyOutput>> outputStreams;
    for (const auto& stream : streams) {
        outputStreams.push_back(CreateZeroCopyOutputStreamWrapper(stream));
    }

    auto pySchemasArg = ExtractArgument(args, kwargs, "py_schemas");
    if (!pySchemasArg.isList()) {
        throw Py::TypeError("\"py_schemas\" should be a list");
    }
    auto pySchemas = Py::List(pySchemasArg);

    auto skiffSchemasArg = ExtractArgument(args, kwargs, "skiff_schemas");
    if (!skiffSchemasArg.isList()) {
        throw Py::TypeError("\"skiff_schemas\" should be a list");
    }
    auto skiffSchemas = Py::List(skiffSchemasArg);
    if (pySchemas.size() != skiffSchemas.size()) {
        throw Py::RuntimeError(Format(
            "Length of \"py_schemas\" and of \"skiff_schemas\" must be the same, got %v != %v",
            pySchemas.size(),
            skiffSchemas.size()));
    }
    
    ValidateArgumentsEmpty(args, kwargs);

    std::vector<TPythonToSkiffConverter> converters;
    std::vector<std::unique_ptr<TCheckedInDebugSkiffWriter>> skiffWriters;
    for (int index = 0; index < streams.size(); ++index) {
        Py::PythonClassObject<TSkiffSchemaPython> schemaPythonObject(skiffSchemas[index]);
        auto skiffSchema = schemaPythonObject.getCxxObject()->GetSchemaObject()->GetSkiffSchema();
        converters.push_back(CreateRowPythonToSkiffConverter(pySchemas[index]));
        auto skiffWriter = std::make_unique<TCheckedInDebugSkiffWriter>(
            CreateVariant16Schema(std::vector{skiffSchema}), outputStreams[index].get());
        skiffWriters.push_back(std::move(skiffWriter));
    }

    auto iterator = CreateIterator(objects);
    while (auto* item_ = PyIter_Next(*iterator)) {
        auto item = Py::Object(item_, /* owned */ true);
        Py::Object actualRow;
        ui16 tableIndex = 0;
        if (IsOutputRow(item)) {
            auto tableIndexObj = item.getAttr("_table_index");
            tableIndex = static_cast<int>(Py::Int(tableIndexObj));
            actualRow = item.getAttr("_row");
        } else {
            if (!IsYtDataclass(item)) {
                throw Py::TypeError(Format(
                    "Expected an object of type yt.wrapper.Row "
                    "or a class marked with @yt_dataclass, got %v",
                    Repr(item)));
            }
            actualRow = std::move(item);
        }
        if (tableIndex >= converters.size()) {
            throw Py::RuntimeError(Format("Invalid table index %v, expected it to be in range [0, %v)",
                tableIndex,
                converters.size()));
        }
        try {
            skiffWriters[tableIndex]->WriteVariant16Tag(0);
            converters[tableIndex](actualRow.ptr(), skiffWriters[tableIndex].get());
        } catch (const TErrorException& exception) {
            throw CreateSkiffError(
                "Python to Skiff conversion failed",
                exception.Error());
        } catch (const std::exception& exception) {
            throw CreateSkiffError(
                "Python to Skiff conversion failed",
                TError(exception));
        }
    }
    if (PyErr_Occurred()) {
        throw Py::Exception();
    }

    for (const auto& skiffWriter : skiffWriters) {
        skiffWriter->Finish();
    }
    return Py::None();
}

Py::Object DumpSkiffStructured(Py::Tuple& args, Py::Dict& kwargs)
{
    try {
        return DumpSkiffStructuredImpl(args, kwargs);
    } catch (const std::exception& exception) {
        throw Py::RuntimeError(Format("Error dumping structured skiff: %v", exception.what()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
