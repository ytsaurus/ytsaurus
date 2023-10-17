#include "converter_python_to_skiff.h"
#include "other_columns.h"

#include <yt/yt/python/common/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/system/yassert.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <exception>
#include <limits>
#include <type_traits>

namespace NYT::NPython {

using namespace NSkiff;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPythonToSkiffConverter CreatePythonToSkiffConverter(TString description, Py::Object pySchema, bool validateOptionalOnRuntime);

////////////////////////////////////////////////////////////////////////////////

template <bool IsValidateOptionalOnRuntime, typename TConverter>
TPythonToSkiffConverter CreateOptionalPythonToSkiffConverter(TString description, TConverter converter, bool isPySchemaOptional, bool isTiSchemaOptional)
{
    if (isTiSchemaOptional) {
        // ti - optional
        // py - any
        return [converter=std::move(converter)] (PyObject* obj, TCheckedInDebugSkiffWriter* writer) mutable {
            if (obj == Py_None) {
                writer->WriteVariant8Tag(0);
            } else {
                writer->WriteVariant8Tag(1);
                converter(obj, writer);
            }
        };
    } else {
        if (isPySchemaOptional) {
            // ti - mandatory
            // py - optional
            return [converter=std::move(converter), description=std::move(description)] (PyObject* obj, TCheckedInDebugSkiffWriter* writer) mutable {
                if constexpr (IsValidateOptionalOnRuntime) {
                    if (obj == Py_None) {
                        THROW_ERROR_EXCEPTION("Malformed dataclass: None in required for field %Qv", description);
                    } else {
                        converter(obj, writer);
                    }
                } else {
                    THROW_ERROR_EXCEPTION("Malformed dataclass: None in required for field %Qv", description);
                }
            };
        } else {
            // ti - mandatory
            // py - mandatory
            return [converter=std::move(converter)] (PyObject* obj, TCheckedInDebugSkiffWriter* writer) mutable {
                converter(obj, writer);
            };
        }
    }
}

template <typename TConverter>
TPythonToSkiffConverter WrapPythonToSkiffConverterImpl(TString description, TConverter converter, bool isPySchemaOptional, bool isTiSchemaOptional, bool validateOptionalOnRuntime)
{
    if (isPySchemaOptional || isTiSchemaOptional) {
        if (validateOptionalOnRuntime) {
            return CreateOptionalPythonToSkiffConverter<true>(std::move(description), std::move(converter), isPySchemaOptional, isTiSchemaOptional);
        } else {
            return CreateOptionalPythonToSkiffConverter<false>(std::move(description), std::move(converter), isPySchemaOptional, isTiSchemaOptional);
        }
    } else {
        return converter;
    }
}

template <typename TConverter>
TPythonToSkiffConverter MaybeWrapPythonToSkiffConverter(TString description, TConverter converter, bool isPySchemaOptional, bool isTiSchemaOptional, bool validateOptionalOnRuntime)
{
    return WrapPythonToSkiffConverterImpl(std::move(description), std::move(converter), isPySchemaOptional, isTiSchemaOptional, validateOptionalOnRuntime);
}

template <EWireType WireType, EPythonType PythonType>
class TPrimitivePythonToSkiffConverter
{
public:
    TPrimitivePythonToSkiffConverter(TString description)
        : Description_(description)
    { }

    void operator() (PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        TError error;
        try {
            Write(obj, writer);
            return;
        } catch (const Py::BaseException& pyException) {
            error = Py::BuildErrorFromPythonException(/*clear*/ true);
        } catch (const std::exception& exception) {
            error = TError(exception);
        }
        THROW_ERROR_EXCEPTION("Failed to write field %Qv of Python type %Qlv as Skiff type %Qlv",
            Description_,
            PythonType,
            WireType)
            << error;
    }

private:
    TString Description_;

private:
    void Write(PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        if constexpr (PythonType == EPythonType::Int) {
            WriteInt(obj, writer);
        } else if constexpr (PythonType == EPythonType::Bytes) {
            static_assert(WireType == EWireType::String32);
            WriteBytes(obj, writer);
        } else if constexpr (PythonType == EPythonType::Str) {
            static_assert(WireType == EWireType::String32);
            // We are parsing either a field of type Utf8 or String encoded in UTF-8.
            auto bytesObj = PyObjectPtr(PyUnicode_AsUTF8String(obj));
            if (!bytesObj) {
                throw Py::Exception();
            }
            WriteBytes(bytesObj.get(), writer);
        } else if constexpr (PythonType == EPythonType::Float) {
            static_assert(WireType == EWireType::Double);
            auto value = PyFloat_AsDouble(obj);
            if (value == -1.0 && PyErr_Occurred()) {
                throw Py::Exception();
            }
            writer->WriteDouble(value);
        } else if constexpr (PythonType == EPythonType::Bool) {
            static_assert(WireType == EWireType::Boolean);
            if (!PyBool_Check(obj)) {
                THROW_ERROR_EXCEPTION("Expected value of type bool, got %Qv",
                    Repr(Py::Object(obj)));
            }
            auto value = (obj == Py_True);
            writer->WriteBoolean(value);
        } else {
            // Silly check instead of static_assert(false);
            static_assert(PythonType == EPythonType::Int);
        }
    }

    template <typename T>
    T CheckAndGetLongLong(PyObject* obj)
    {
        static_assert(std::is_integral_v<T>);
        if (!PyLong_Check(obj)) {
            THROW_ERROR_EXCEPTION("Expected value of type int, got %v",
                Repr(Py::Object(obj)));
        }
        auto extractValue = [&] {
            if constexpr (std::is_signed_v<T>) {
                return PyLong_AsLongLong(obj);
            } else {
                return PyLong_AsUnsignedLongLong(obj);
            }
        };
        auto value = extractValue();
        if (value == static_cast<decltype(value)>(-1) && PyErr_Occurred()) {
            auto exception = Py::BuildErrorFromPythonException(/*clear*/ true);
            THROW_ERROR_EXCEPTION("Got too large integer value %v",
                Repr(Py::Object(obj)))
                << exception;
        }
        if (value > std::numeric_limits<T>::max() || value < std::numeric_limits<T>::min()) {
            THROW_ERROR_EXCEPTION("Got integer value %v out of range [%v, %v]",
                value,
                std::numeric_limits<T>::min(),
                std::numeric_limits<T>::max());
        }
        return static_cast<T>(value);
    }

    void WriteBytes(PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        auto str = Py::ConvertToStringBuf(obj);
        writer->WriteString32(str);
    }

    void WriteInt(PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        static_assert(PythonType == EPythonType::Int);
        if constexpr (WireType == EWireType::Int8) {
            auto value = CheckAndGetLongLong<i8>(obj);
            writer->WriteInt8(value);
        } else if constexpr (WireType == EWireType::Int16) {
            auto value = CheckAndGetLongLong<i16>(obj);
            writer->WriteInt16(value);
        } else if constexpr (WireType == EWireType::Int32) {
            auto value = CheckAndGetLongLong<i32>(obj);
            writer->WriteInt32(value);
        } else if constexpr (WireType == EWireType::Int64) {
            auto value = CheckAndGetLongLong<i64>(obj);
            writer->WriteInt64(value);
        } else if constexpr (WireType == EWireType::Uint8) {
            auto value = CheckAndGetLongLong<ui8>(obj);
            writer->WriteUint8(value);
        } else if constexpr (WireType == EWireType::Uint16) {
            auto value = CheckAndGetLongLong<ui16>(obj);
            writer->WriteUint16(value);
        } else if constexpr (WireType == EWireType::Uint32) {
            auto value = CheckAndGetLongLong<ui32>(obj);
            writer->WriteUint32(value);
        } else if constexpr (WireType == EWireType::Uint64) {
            auto value = CheckAndGetLongLong<ui64>(obj);
            writer->WriteUint64(value);
        } else {
            // Silly check instead of static_assert(false);
            static_assert(WireType == EWireType::Int8);
        }
    }
};

template <bool IsValidateOptionalOnRuntime>
TPythonToSkiffConverter CreatePrimitivePythonToSkiffConverterImpl(TString description, Py::Object pySchema, EPythonType pythonType, bool isPySchemaOptional, bool isTiSchemaOptional)
{
    auto wireTypeStr = Py::ConvertStringObjectToString(GetAttr(pySchema, WireTypeFieldName));
    auto wireType = ::FromString<EWireType>(wireTypeStr);

    switch (pythonType) {
        case EPythonType::Int:
            switch (wireType) {
#define CASE(WireType) \
                case WireType: { \
                    auto converter = TPrimitivePythonToSkiffConverter<WireType, EPythonType::Int>(description); \
                    return MaybeWrapPythonToSkiffConverter( \
                        std::move(description), \
                        std::move(converter), \
                        isPySchemaOptional, \
                        isTiSchemaOptional, \
                        IsValidateOptionalOnRuntime \
                    ); \
                }

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
                    THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Unexpected wire type %Qlv for \"int\" python type",
                        wireType);
            }
#define CASE(PythonType, WireType) \
        case PythonType: { \
                auto converter = TPrimitivePythonToSkiffConverter<WireType, PythonType>(description); \
                return MaybeWrapPythonToSkiffConverter( \
                    std::move(description), \
                    std::move(converter), \
                    isPySchemaOptional, \
                    isTiSchemaOptional, \
                    IsValidateOptionalOnRuntime \
                ); \
            }

        CASE(EPythonType::Bytes, EWireType::String32)
        CASE(EPythonType::Str, EWireType::String32)
        CASE(EPythonType::Float, EWireType::Double)
        CASE(EPythonType::Bool, EWireType::Boolean)
#undef CASE
    }
    YT_ABORT();
}

TPythonToSkiffConverter WrapWithMiddlewareConverter(TPythonToSkiffConverter converter, Py::Callable middlewareConverter)
{
    return [converter = std::move(converter), middlewareConverter = std::move(middlewareConverter)](PyObject* obj, TCheckedInDebugSkiffWriter* writer) mutable {
        Py::Tuple args(1);
        args[0] = Py::Object(obj);
        auto pyBaseObject = middlewareConverter.apply(args);
        return converter(pyBaseObject.ptr(), writer);
    };
}

TPythonToSkiffConverter CreatePrimitivePythonToSkiffConverter(TString description, Py::Object pySchema, bool isPySchemaOptional, bool isTiSchemaOptional, bool validateOptionalOnRuntime)
{
    auto middlewareTypeConverter = GetAttr(pySchema, "_to_yt_type");

    EPythonType pythonType;
    if (middlewareTypeConverter.isNone()) {
        pythonType = GetPythonType(GetAttr(pySchema, PyTypeFieldName));
    } else {
        pythonType = GetPythonType(GetAttr(pySchema, PyWireTypeFieldName));
    }

    TPythonToSkiffConverter primitiveConverter;

    if (validateOptionalOnRuntime) {
        primitiveConverter = CreatePrimitivePythonToSkiffConverterImpl<true>(
            description,
            std::move(pySchema),
            pythonType,
            isPySchemaOptional,
            isTiSchemaOptional);
    } else {
        primitiveConverter = CreatePrimitivePythonToSkiffConverterImpl<false>(
            description,
            std::move(pySchema),
            pythonType,
            isPySchemaOptional,
            isTiSchemaOptional);
    }

    if (middlewareTypeConverter.isNone()) {
        return primitiveConverter;
    }
    return WrapWithMiddlewareConverter(std::move(primitiveConverter), Py::Callable(std::move(middlewareTypeConverter)));
}

class TStructPythonToSkiffConverter
{
public:
    TStructPythonToSkiffConverter(TString description, Py::Object pySchema, bool validateOptionalOnRuntime)
        : Description_(description)
    {
        static auto StructFieldClass = GetSchemaType("StructField");
        auto fields = Py::List(GetAttr(pySchema, FieldsFieldName));
        for (const auto& field : fields) {
            if (PyObject_IsInstance(field.ptr(), StructFieldClass.get())) {
                auto fieldName = Py::ConvertStringObjectToString(GetAttr(field, NameFieldName));
                auto fieldDescription = Description_ + "." + fieldName;
                FieldConverters_.push_back(
                    CreatePythonToSkiffConverter(
                        fieldDescription,
                        GetAttr(field, PySchemaFieldName),
                        validateOptionalOnRuntime
                    )
                );
                FieldNames_.push_back(fieldName);
            }
        }
    }

    void operator() (PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        for (int i = 0; i < std::ssize(FieldConverters_); ++i) {
            auto field = PyObjectPtr(PyObject_GetAttrString(obj, FieldNames_[i].c_str()));
            if (!field) {
                THROW_ERROR_EXCEPTION("Failed to get field \"%v.%v\"",
                    Description_,
                    FieldNames_[i])
                    << Py::BuildErrorFromPythonException(/*clear*/ true);
            }
            FieldConverters_[i](field.get(), writer);
        }

    }

private:
    TString Description_;
    std::vector<TPythonToSkiffConverter> FieldConverters_;
    std::vector<TString> FieldNames_;
};

class TListPythonToSkiffConverter
{
public:
    explicit TListPythonToSkiffConverter(TString description, Py::Object pySchema, bool validateOptionalOnRuntime)
        : Description_(description)
        , ItemConverter_(
            CreatePythonToSkiffConverter(
                Description_ + ".<list-element>",
                GetAttr(pySchema, ItemFieldName),
                validateOptionalOnRuntime
            )
        )
    { }

    void operator() (PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        auto iterator = PyObjectPtr(PyObject_GetIter(obj));
        if (!iterator) {
            THROW_ERROR_EXCEPTION("Failed to iterate over %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        while (auto item = PyObjectPtr(PyIter_Next(iterator.get()))) {
            writer->WriteVariant8Tag(0);
            ItemConverter_(item.get(), writer);
        }
        if (PyErr_Occurred()) {
            THROW_ERROR_EXCEPTION("Error occurred during iteration over %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        writer->WriteVariant8Tag(EndOfSequenceTag<ui8>());
    }

private:
    TString Description_;
    TPythonToSkiffConverter ItemConverter_;
};

class TTuplePythonToSkiffConverter
{
public:
    explicit TTuplePythonToSkiffConverter(TString description, Py::Object pySchema, bool validateOptionalOnRuntime)
        : Description_(description)
    {
        int i = 0;
        for (const auto& pyElementSchema : Py::List(GetAttr(pySchema, ElementsFieldName))) {
            ElementConverters_.push_back(
                CreatePythonToSkiffConverter(Format("%v.<tuple-element-%v>", description, i), pyElementSchema, validateOptionalOnRuntime)
            );
            i += 1;
        }
    }

    void operator() (PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        for (int i = 0; i < std::ssize(ElementConverters_); i++) {
            auto element = PyTuple_GetItem(obj, i);
            if (!element) {
                THROW_ERROR_EXCEPTION("Failed to get item from tuple %Qv",
                    Description_)
                    << Py::BuildErrorFromPythonException(/*clear*/ true);
            }
            ElementConverters_[i](element, writer);
        }
        if (PyErr_Occurred()) {
            THROW_ERROR_EXCEPTION("Error occurred during iteration over %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
    }

private:
    TString Description_;
    std::vector<TPythonToSkiffConverter> ElementConverters_;
};

class TDictPythonToSkiffConverter
{
public:
    explicit TDictPythonToSkiffConverter(TString description, Py::Object pySchema, bool validateOptionalOnRuntime)
        : Description_(description)
        , KeyConverter_(CreatePythonToSkiffConverter(
            Description_ + ".<key>",
            GetAttr(pySchema, KeyFieldName),
            validateOptionalOnRuntime))
        , ValueConverter_(CreatePythonToSkiffConverter(
            Description_ + ".<value>",
            GetAttr(pySchema, ValueFieldName),
            validateOptionalOnRuntime))
    { }

    void operator() (PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(obj, &pos, &key, &value)) {
            writer->WriteVariant8Tag(0);
            KeyConverter_(key, writer);
            ValueConverter_(value, writer);
        }
        if (PyErr_Occurred()) {
            THROW_ERROR_EXCEPTION("Error occurred during iteration over %Qv",
                Description_)
                << Py::BuildErrorFromPythonException(/*clear*/ true);
        }
        writer->WriteVariant8Tag(EndOfSequenceTag<ui8>());
    }

private:
    TString Description_;
    TPythonToSkiffConverter KeyConverter_;
    TPythonToSkiffConverter ValueConverter_;
};

class TRowPythonToSkiffConverter
{
public:
    explicit TRowPythonToSkiffConverter(Py::Object pySchema)
        : RowClassName_(GetRowClassName(pySchema))
        , ValidateOptionalOnRuntime_(
            FindAttr(pySchema, SchemaRuntimeContextFieldName)
            && GetAttr(GetAttr(pySchema, SchemaRuntimeContextFieldName), ValidateOptionalOnRuntimeFieldName).as_bool()
        )
        , StructConverter_(RowClassName_, GetAttr(pySchema, StructSchemaFieldName), ValidateOptionalOnRuntime_)
    {
        auto otherColumnsField = GetAttr(GetAttr(pySchema, StructSchemaFieldName), OtherColumnsFieldFieldName);
        if (!otherColumnsField.isNone()) {
            OtherColumnsFieldName_ = GetAttr(otherColumnsField, NameFieldName).as_string();
        }
    }

    void operator() (PyObject* obj, TCheckedInDebugSkiffWriter* writer)
    {
        StructConverter_(obj, writer);

        if (OtherColumnsFieldName_) {
            auto field = PyObjectPtr(PyObject_GetAttrString(obj, OtherColumnsFieldName_->c_str()));
            if (!field) {
                THROW_ERROR_EXCEPTION("Failed to get OtherColumns field \"%v.%v\"",
                    RowClassName_,
                    OtherColumnsFieldName_)
                    << Py::BuildErrorFromPythonException(/*clear*/ true);
            }
            if (field.get() == Py_None) {
                static constexpr TStringBuf EmptyMapYson = "{}";
                writer->WriteYson32(EmptyMapYson);
            } else {
                Py::PythonClassObject<TSkiffOtherColumns> otherColumns(field.get());
                writer->WriteYson32(otherColumns.getCxxObject()->GetYsonString().AsStringBuf());
            }
        }
    }

private:
    TString RowClassName_;
    bool ValidateOptionalOnRuntime_;
    TStructPythonToSkiffConverter StructConverter_;
    std::optional<TString> OtherColumnsFieldName_;
};

TPythonToSkiffConverter CreateRowPythonToSkiffConverter(Py::Object pySchema)
{
    return TRowPythonToSkiffConverter(std::move(pySchema));
}

TPythonToSkiffConverter CreatePythonToSkiffConverterImpl(TString description, Py::Object pySchema, bool isPySchemaOptional, bool isTiSchemaOptional, bool validateOptionalOnRuntime)
{
    static auto StructSchemaClass = GetSchemaType("StructSchema");
    static auto PrimitiveSchemaClass = GetSchemaType("PrimitiveSchema");
    static auto OptionalSchemaClass = GetSchemaType("OptionalSchema");
    static auto ListSchemaClass = GetSchemaType("ListSchema");
    static auto TupleSchemaClass = GetSchemaType("TupleSchema");
    static auto DictSchemaClass = GetSchemaType("DictSchema");

    if (PyObject_IsInstance(pySchema.ptr(), StructSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter(
            std::move(description),
            TStructPythonToSkiffConverter(description, pySchema, validateOptionalOnRuntime),
            isPySchemaOptional,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else if (PyObject_IsInstance(pySchema.ptr(), PrimitiveSchemaClass.get())) {
        return CreatePrimitivePythonToSkiffConverter(
            std::move(description),
            std::move(pySchema),
            isPySchemaOptional,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else if (PyObject_IsInstance(pySchema.ptr(), OptionalSchemaClass.get())) {
        if (isPySchemaOptional) {
            THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Optional[Optional[T]] is not allowed");
        }
        bool isTiSchemaOptional = IsTiTypeOptional(pySchema);
        return CreatePythonToSkiffConverterImpl(
            description + ".<optional-element>",
            GetAttr(pySchema, ItemFieldName),
            true,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else if (PyObject_IsInstance(pySchema.ptr(), ListSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter(
            std::move(description),
            TListPythonToSkiffConverter(description, pySchema, validateOptionalOnRuntime),
            isPySchemaOptional,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else if (PyObject_IsInstance(pySchema.ptr(), TupleSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter(
            std::move(description),
            TTuplePythonToSkiffConverter(description, pySchema, validateOptionalOnRuntime),
            isPySchemaOptional,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else if (PyObject_IsInstance(pySchema.ptr(), DictSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter(
            std::move(description),
            TDictPythonToSkiffConverter(description, pySchema, validateOptionalOnRuntime),
            isPySchemaOptional,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else {
        THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Unknown schema type %Qv",
            Repr(pySchema.type()));
    }
}

TPythonToSkiffConverter CreatePythonToSkiffConverter(TString description, Py::Object pySchema, bool validateOptionalOnRuntime)
{
    static auto OptionalSchemaClass = GetSchemaType("OptionalSchema");
    bool isTiSchemaOptional = IsTiTypeOptional(pySchema);
    bool isPySchemaOptional = PyObject_IsInstance(pySchema.ptr(), OptionalSchemaClass.get());

    if (isPySchemaOptional) {
        if (!isTiSchemaOptional && !validateOptionalOnRuntime) {
            THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Can not write optional "
                "python field %Qv to non-optional schema field",
                description);
        }
        return CreatePythonToSkiffConverterImpl(
            description + ".<optional-element>",
            GetAttr(pySchema, ItemFieldName),
            true,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    } else {
        return CreatePythonToSkiffConverterImpl(
            description,
            std::move(pySchema),
            false,
            isTiSchemaOptional,
            validateOptionalOnRuntime);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
