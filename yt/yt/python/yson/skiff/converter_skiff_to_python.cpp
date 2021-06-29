#include "converter_skiff_to_python.h"
#include "Python2/Objects.hxx"
#include "contrib/tools/python/src/Include/object.h"

#include <yt/yt/python/common/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/system/yassert.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <limits>
#include <type_traits>

namespace NYT::NPython {

using namespace NSkiff;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSkiffToPythonConverter CreateSkiffToPythonConverter(TString description, Py::Object pySchema);

////////////////////////////////////////////////////////////////////////////////

template <typename TConverter>
TSkiffToPythonConverter CreateOptionalSkiffToPythonConverter(TConverter converter)
{
    return [converter=std::move(converter)] (TCheckedInDebugSkiffParser* parser) mutable {
        auto tag = parser->ParseVariant8Tag();
        switch (tag) {
            case 0:
                Py_IncRef(Py_None);
                return PyObjectPtr(Py_None);
            case 1:
                return converter(parser);
            default:
                THROW_ERROR_EXCEPTION("Expected variant8 tag in range [0, 2), got %v", tag);
        }
    };
}

template <typename TConverter>
TSkiffToPythonConverter MaybeWrapSkiffToPythonConverter(
    const Py::Object& pySchema,
    TConverter converter,
    bool forceOptional = false)
{
    if (forceOptional) {
        Y_VERIFY(!IsTiTypeOptional(pySchema));
        return CreateOptionalSkiffToPythonConverter(std::move(converter));
    }
    if (IsTiTypeOptional(pySchema)) {
        return CreateOptionalSkiffToPythonConverter(std::move(converter));
    } else {
        return converter;
    }
}

class TOtherColumnsSkiffToPythonConverter
{
public:
    TOtherColumnsSkiffToPythonConverter(TString description)
        : OtherColumnsClass_(TSkiffOtherColumns::type())
        , OtherColumnsArgs_(1)
        , Description_(std::move(description))
    { }

    PyObjectPtr operator() (TCheckedInDebugSkiffParser* parser) 
    {
        auto string = parser->ParseString32();
        auto bytes = PyObjectPtr(PyBytes_FromStringAndSize(string.begin(), string.size()));
        if (!bytes) {
            THROW_ERROR_EXCEPTION("Failed to create bytes for field %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        if (PyTuple_SetItem(OtherColumnsArgs_.ptr(), 0, bytes.get()) == -1) {
            THROW_ERROR_EXCEPTION("Failed to set tuple element for constructor of field %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        auto obj = PyObjectPtr(PyObject_Call(OtherColumnsClass_.ptr(), OtherColumnsArgs_.ptr(), /* kwargs */ nullptr));
        if (!obj) {
            THROW_ERROR_EXCEPTION("Failed to create OtherColumns field %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        return obj;
    }

private:
    Py::Callable OtherColumnsClass_;
    Py::Tuple OtherColumnsArgs_;
    TString Description_;
};

template <EWireType WireType, EPythonType PythonType>
class TPrimitiveSkiffToPythonConverter
{
public:
    TPrimitiveSkiffToPythonConverter(TString description)
        : Description_(std::move(description))
    { }

    PyObjectPtr operator() (TCheckedInDebugSkiffParser* parser)
    {
        PyObjectPtr result;
        try {
            result = PyObjectPtr(DoConvert(parser));
        } catch (const std::exception& exception) {
            THROW_ERROR_EXCEPTION("Failed to parse field %Qv of Python type %Qlv from wire type %Qlv",
                Description_,
                PythonType,
                WireType)
                << TError(exception);
        }
        if (!result) {
            THROW_ERROR_EXCEPTION("Failed to parse field %Qv of Python type %Qlv from wire type %Qlv",
                Description_,
                PythonType,
                WireType)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        return result;
    }

private:
    TString Description_;

private:
    PyObject* DoConvert(TCheckedInDebugSkiffParser* parser)
    {
        if constexpr (PythonType == EPythonType::Int) {
            return ParseInt(parser);
        } else if constexpr (PythonType == EPythonType::Bytes) {
            static_assert(WireType == EWireType::String32);
            auto string = parser->ParseString32();
            return PyBytes_FromStringAndSize(string.begin(), string.size());
        } else if constexpr (PythonType == EPythonType::Str) {
            static_assert(WireType == EWireType::String32);
            auto string = parser->ParseString32();
            return PyUnicode_FromStringAndSize(string.begin(), string.size());
        } else if constexpr (PythonType == EPythonType::Float) {
            static_assert(WireType == EWireType::Double);
            auto number = parser->ParseDouble();
            return PyFloat_FromDouble(number);
        } else if constexpr (PythonType == EPythonType::Bool) {
            static_assert(WireType == EWireType::Boolean);
            auto value = parser->ParseBoolean();
            return PyBool_FromLong(value);
        } else {
            // Silly check instead of static_assert(false);
            static_assert(PythonType == EPythonType::Int);
        }
    }

    PyObject* ParseInt(TCheckedInDebugSkiffParser* parser)
    {
        if constexpr (WireType == EWireType::Int8) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromLongLong(parser->ParseInt8());
        } else if constexpr (WireType == EWireType::Int16) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromLongLong(parser->ParseInt16());
        } else if constexpr (WireType == EWireType::Int32) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromLongLong(parser->ParseInt32());
        } else if constexpr (WireType == EWireType::Int64) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromLongLong(parser->ParseInt64());
        } else if constexpr (WireType == EWireType::Uint8) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromUnsignedLongLong(parser->ParseUint8());
        } else if constexpr (WireType == EWireType::Uint16) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromUnsignedLongLong(parser->ParseUint16());
        } else if constexpr (WireType == EWireType::Uint32) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromUnsignedLongLong(parser->ParseUint32());
        } else if constexpr (WireType == EWireType::Uint64) {
            static_assert(PythonType == EPythonType::Int);
            return PyLong_FromUnsignedLongLong(parser->ParseUint64());
        } else {
            // Silly check instead of static_assert(false);
            static_assert(WireType == EWireType::Int8);
        }
    }
};

template <EWireType WireType, EPythonType PythonType>
TSkiffToPythonConverter CreatePrimitiveSkiffToPythonConverterImpl(
    TString description,
    Py::Object pySchema,
    bool forceOptional)
{
    auto converter = TPrimitiveSkiffToPythonConverter<WireType, PythonType>(std::move(description));
    return MaybeWrapSkiffToPythonConverter(std::move(pySchema), std::move(converter), forceOptional);
}

TSkiffToPythonConverter CreatePrimitiveSkiffToPythonConverter(
    TString description,
    Py::Object pySchema,
    bool forceOptional = false)
{
    auto wireTypeStr = Py::ConvertStringObjectToString(GetAttr(pySchema, WireTypeFieldName));
    auto pythonType = GetPythonType(GetAttr(pySchema, PyTypeFieldName));
    auto wireType = ::FromString<EWireType>(wireTypeStr);
    switch (pythonType) {
        case EPythonType::Int:
            switch (wireType) {
#define CASE(WireType) \
                case WireType: \
                    return CreatePrimitiveSkiffToPythonConverterImpl<WireType, EPythonType::Int>( \
                        std::move(description), \
                        std::move(pySchema), \
                        forceOptional);
                CASE(EWireType::Int8)
                CASE(EWireType::Int16)
                CASE(EWireType::Int32)
                CASE(EWireType::Int64)
                CASE(EWireType::Uint8)
                CASE(EWireType::Uint16)
                CASE(EWireType::Uint32)
                CASE(EWireType::Uint64)
#undef CASE
                default:
                    THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Unexpected wire type %Qlv for int python type",
                        wireType);
            }

        case EPythonType::Str:
            return CreatePrimitiveSkiffToPythonConverterImpl<EWireType::String32, EPythonType::Str>(
                std::move(description),
                std::move(pySchema),
                forceOptional);
        case EPythonType::Bytes:
            return CreatePrimitiveSkiffToPythonConverterImpl<EWireType::String32, EPythonType::Bytes>(
                std::move(description),
                std::move(pySchema),
                forceOptional);
        case EPythonType::Float:
            return CreatePrimitiveSkiffToPythonConverterImpl<EWireType::Double, EPythonType::Float>(
                std::move(description),
                std::move(pySchema),
                forceOptional);
        case EPythonType::Bool:
            return CreatePrimitiveSkiffToPythonConverterImpl<EWireType::Boolean, EPythonType::Bool>(
                std::move(description),
                std::move(pySchema),
                forceOptional);
    }
    Y_FAIL();
}

class TStructSkiffToPythonConverter
{
public:
    explicit TStructSkiffToPythonConverter(TString description, Py::Object pySchema)
        : Description_(description)
    {
        static auto StructFieldClass = GetSchemaType("StructField");
        static auto FieldMissingFromSchemaClass = GetSchemaType("FieldMissingFromSchema"); 
        PyType_ = GetAttr(pySchema, PyTypeFieldName);
        auto fields = Py::List(GetAttr(pySchema, FieldsFieldName));
        for (const auto& field : fields) {
            if (PyObject_IsInstance(field.ptr(), StructFieldClass.get())) {
                auto fieldName = Py::ConvertStringObjectToString(GetAttr(field, NameFieldName));
                auto fieldDescription = Description_ + "." + fieldName;
                FieldConverters_.push_back(CreateSkiffToPythonConverter(fieldDescription, GetAttr(field, PySchemaFieldName)));
                FieldNames_.push_back(fieldName);
            } else if (PyObject_IsInstance(field.ptr(), FieldMissingFromSchemaClass.get())) {
                FieldsMissingFromSchema_.emplace_back(GetAttr(field, NameFieldName).as_string());
            }
        }
    }

    PyObjectPtr operator() (TCheckedInDebugSkiffParser* parser)
    {
        //auto type = reinterpret_cast<PyTypeObject*>(PyType_.ptr());
        Py::Tuple t(1);
        t[0] = PyType_;
        auto obj = PyObjectPtr(Py::new_reference_to(PyType_.callMemberFunction("__new__", t)));
        if (!obj) {
            THROW_ERROR_EXCEPTION("Failed to create field %Qv of class %Qv",
                Description_,
                PyType_.as_string())
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        for (int i = 0; i < std::ssize(FieldConverters_); ++i) {
            auto field = PyObjectPtr(FieldConverters_[i](parser));
            if (PyObject_SetAttrString(obj.get(), FieldNames_[i].c_str(), field.get()) == -1) {
                THROW_ERROR_EXCEPTION("Failed to set field \"%v.%v\"",
                    Description_,
                    FieldNames_[i])
                    << Py::BuildErrorFromPythonException(/*clear*/ true);
            }
        }
        for (const auto& fieldName : FieldsMissingFromSchema_) {
            if (PyObject_SetAttrString(obj.get(), fieldName.c_str(), Py_None) == -1) {
                THROW_ERROR_EXCEPTION("Failed to set missing field \"%v.%v\"",
                    Description_,
                    fieldName)
                    << Py::BuildErrorFromPythonException(/*clear*/ true);
            }
        }
        return obj;
    }

private:
    TString Description_;
    std::vector<TSkiffToPythonConverter> FieldConverters_;
    std::vector<TString> FieldNames_;
    Py::Object PyType_;
    Py::Tuple EmptyTuple_;
    std::vector<TString> FieldsMissingFromSchema_;
};

TSkiffToPythonConverter CreateStructSkiffToPythonConverter(
    TString description,
    Py::Object pySchema,
    bool forceOptional = false)
{
    auto converter = TStructSkiffToPythonConverter(description, pySchema);
    return MaybeWrapSkiffToPythonConverter(pySchema, std::move(converter), forceOptional);
}

class TListSkiffToPythonConverter
{
public:
    explicit TListSkiffToPythonConverter(TString description, Py::Object pySchema)
        : Description_(description)
        , ItemConverter_(CreateSkiffToPythonConverter(
            description + ".<list-element>",
            GetAttr(pySchema, ItemFieldName)))
    { }

    PyObjectPtr operator() (TCheckedInDebugSkiffParser* parser)
    {
        auto list = PyObjectPtr(PyList_New(0));
        if (!list) {
            THROW_ERROR_EXCEPTION("Failed to create list for field %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        while (true) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                auto item = ItemConverter_(parser);
                PyList_Append(list.get(), item.get());
            } else if (tag == EndOfSequenceTag<ui8>()) {
                break;
            } else {
                THROW_ERROR_EXCEPTION("Expected tag 0 or %v for repeated_variant8, got %v",
                    EndOfSequenceTag<ui8>(),
                    tag);
            }
        }
        return list;
    }

private:
    TString Description_;
    TSkiffToPythonConverter ItemConverter_;
};

TSkiffToPythonConverter CreateListSkiffToPythonConverter(TString description, Py::Object pySchema, bool forceOptional = false)
{
    auto converter = TListSkiffToPythonConverter(description, pySchema);
    return MaybeWrapSkiffToPythonConverter(pySchema, std::move(converter), forceOptional);
}

class TRowSkiffToPythonConverterImpl
{
public:
    explicit TRowSkiffToPythonConverterImpl(Py::Object pySchema)
        : RowClassName_(GetRowClassName(pySchema))
        , StructConverter_(RowClassName_, GetAttr(pySchema, StructSchemaFieldName))
    {
        auto systemColumns = Py::Tuple(GetAttr(pySchema, SystemColumnsFieldName));
        auto iter = std::begin(systemColumns);
        auto validateSystemColumnAndAdvance = [&] (TStringBuf expected) {
            if (Py::Object(*iter).as_string() != expected) {
                THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. System column order mismatch for column %Qv",
                    expected);
            }
            ++iter;
        };
        auto attributes = Py::Dict(GetAttr(pySchema, ControlAttributesFieldName));

        validateSystemColumnAndAdvance("key_switch");
        HasKeySwitch_ = attributes.hasKey("enable_key_switch") && attributes.getItem("enable_key_switch").as_bool();
        
        validateSystemColumnAndAdvance("row_index");
        HasRowIndex_ = attributes.hasKey("enable_row_index") && attributes.getItem("enable_row_index").as_bool();
        
        validateSystemColumnAndAdvance("range_index");
        HasRangeIndex_ = attributes.hasKey("enable_range_index") && attributes.getItem("enable_range_index").as_bool();
        
        validateSystemColumnAndAdvance("other_columns");
        auto otherColumnsField = GetAttr(GetAttr(pySchema, StructSchemaFieldName), OtherColumnsFieldFieldName);
        if (!otherColumnsField.isNone()) {
            OtherColumnsFieldName_ = GetAttr(otherColumnsField, NameFieldName).as_string();
            OtherColumnsConverter_.emplace(RowClassName_ + "." + OtherColumnsFieldName_);
        }
    }

    PyObjectPtr operator() (TCheckedInDebugSkiffParser* parser, TSkiffRowContext* context)
    {
        auto obj = StructConverter_(parser);
        try {
            DoParseSystemColumns(parser, context, obj.get());
        } catch (const std::exception& exception) {
            THROW_ERROR_EXCEPTION("Failed to parse system columns for class %Qv from Skiff",
                RowClassName_)
                << TError(exception);
        }
        return obj;
    }

private:
    TString RowClassName_;
    TStructSkiffToPythonConverter StructConverter_;

    bool HasKeySwitch_ = false;
    bool HasRowIndex_ = false;
    bool HasRangeIndex_ = false;
    TString OtherColumnsFieldName_;
    std::optional<TOtherColumnsSkiffToPythonConverter> OtherColumnsConverter_;

private:
    void DoParseSystemColumns(TCheckedInDebugSkiffParser* parser, TSkiffRowContext* context, PyObject* obj)
    {
        // NB. Keep the order of system columns in sync with constructor.
        if (HasKeySwitch_) {
            context->KeySwitch = parser->ParseBoolean();
        }
        if (HasRowIndex_) {
            if (auto optionalRowIndex = ParseOptionalInt64(parser)) {
                context->RowIndex = *optionalRowIndex;
            }
        }
        if (HasRangeIndex_) {
            if (auto optionalRangeIndex = ParseOptionalInt64(parser)) {
                context->RangeIndex = *optionalRangeIndex;
            }
        }
        if (OtherColumnsConverter_) {
            auto field = PyObjectPtr((*OtherColumnsConverter_)(parser));
            if (PyObject_SetAttrString(obj, OtherColumnsFieldName_.c_str(), field.get()) == -1) {
                throw Py::Exception();
            }
        }
    }

    std::optional<i64> ParseOptionalInt64(TCheckedInDebugSkiffParser* parser)
    {
        auto tag = parser->ParseVariant8Tag();
        switch (tag) {
            case 0:
                return std::nullopt;
            case 1:
                return parser->ParseInt64();
            default:
                THROW_ERROR_EXCEPTION("Expected variant8 tag in range [0, 2), got %v", tag);
        }
    }
};

TRowSkiffToPythonConverter CreateRowSkiffToPythonConverter(Py::Object pySchema)
{
    return TRowSkiffToPythonConverterImpl(std::move(pySchema));
}

TSkiffToPythonConverter CreateSkiffToPythonConverter(TString description, Py::Object pySchema)
{
    static auto StructSchemaClass = GetSchemaType("StructSchema");
    static auto PrimitiveSchemaClass = GetSchemaType("PrimitiveSchema");
    static auto OptionalSchemaClass = GetSchemaType("OptionalSchema");
    static auto ListSchemaClass = GetSchemaType("ListSchema");

    if (PyObject_IsInstance(pySchema.ptr(), PrimitiveSchemaClass.get())) {
        return CreatePrimitiveSkiffToPythonConverter(description, pySchema);
    } else if (PyObject_IsInstance(pySchema.ptr(), StructSchemaClass.get())) {
        return CreateStructSkiffToPythonConverter(description, pySchema);
    } else if (PyObject_IsInstance(pySchema.ptr(), OptionalSchemaClass.get())) {
        auto elementDescription = description + ".<optional-element>";
        auto item = GetAttr(pySchema, ItemFieldName);
        if (!IsTiTypeOptional(pySchema)) {
            return CreateSkiffToPythonConverter(elementDescription, item);
        }
        if (IsTiTypeOptional(item)) {
            // Optional[Optional], slow case.
            return CreateOptionalSkiffToPythonConverter(CreateSkiffToPythonConverter(elementDescription, item));
        }
        if (PyObject_IsInstance(item.ptr(), PrimitiveSchemaClass.get())) {
            return CreatePrimitiveSkiffToPythonConverter(elementDescription, item, /* forceOptional */ true);
        } else if (PyObject_IsInstance(pySchema.ptr(), StructSchemaClass.get())) {
            return CreateStructSkiffToPythonConverter(elementDescription, item, /* forceOptional */ true);
        } else if (PyObject_IsInstance(pySchema.ptr(), ListSchemaClass.get())) {
            return CreateListSkiffToPythonConverter(elementDescription, item, /* forceOptional */ true);
        } else {
            return CreateOptionalSkiffToPythonConverter(CreateSkiffToPythonConverter(elementDescription, item));
        }
    } else if (PyObject_IsInstance(pySchema.ptr(), ListSchemaClass.get())) {
        return CreateListSkiffToPythonConverter(description, pySchema);
    } else {
        Y_FAIL();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython