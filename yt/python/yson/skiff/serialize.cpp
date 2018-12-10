#include "serialize.h"
#include "schema.h"
#include "record.h"
#include "switch.h"
#include "../serialize.h"
#include "../helpers.h"

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/ref_counted_tracker.h>

#include <yt/core/skiff/skiff.h>

namespace NYT {
namespace NPython {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SerializeField(
    const TString& name,
    NSkiff::TSkiffSchemaPtr schema,
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
    switch (schema->GetWireType()) {
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
            auto value = ConvertToStringBuf(EncodeStringObject(object, encoding));
            skiffWriter->WriteString32(value);
            break;
        }
        case NSkiff::EWireType::Yson32: {
            auto value = ConvertToYsonString(object, EYsonFormat::Text);
            skiffWriter->WriteYson32(value.GetData());
            break;
        }
        default:
            Y_UNREACHABLE();
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
        const auto& fieldInfo = schema->GetDenceField(idx);
        const auto& object = record->GetDenseField(idx);

        SerializeField(fieldInfo.Name, fieldInfo.DeoptionalizedSchema, object, fieldInfo.Required, encoding, skiffWriter);
    }

    if (schema->GetSparseFieldsCount() > 0) {
        for (ui16 idx = 0; idx < schema->GetSparseFieldsCount(); ++idx) {
            const auto& fieldInfo = schema->GetSparseField(idx);
            const auto& object = record->GetSparseField(idx + schema->GetDenseFieldsCount());

            if (object.isNone()) {
                continue;
            }

            skiffWriter->WriteVariant16Tag(idx);
            SerializeField(fieldInfo.Name, fieldInfo.DeoptionalizedSchema, object, /* required */ true, encoding, skiffWriter);
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
            /* enableRaw */ false,
            /* booleanAsString */ false);

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

    std::vector<std::unique_ptr<IOutputStream>> outputStreams;
    for (const auto& stream : streams) {
        outputStreams.push_back(CreateOutputStreamWrapper(stream, /* addBuffering */ true));
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
    for (size_t index = 0; index < streams.size(); ++index) {
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

} // namespace NPython
} // namespace NYT
