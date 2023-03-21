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

TPythonToSkiffConverter CreatePythonToSkiffConverter(TString description, Py::Object pySchema);

////////////////////////////////////////////////////////////////////////////////

template <bool IsPySchemaOptional, typename TConverter>
TPythonToSkiffConverter CreateOptionalPythonToSkiffConverter(TConverter converter)
{
    return [converter=std::move(converter)] (PyObject* obj, TCheckedInDebugSkiffWriter* writer) mutable {
        if constexpr (IsPySchemaOptional) {
            if (obj == Py_None) {
                writer->WriteVariant8Tag(0);
            } else {
                writer->WriteVariant8Tag(1);
                converter(obj, writer);
            }
        } else {
            if (Y_UNLIKELY(obj == Py_None)) {
                THROW_ERROR_EXCEPTION("Malformed dataclass: None in required field");
            }
            writer->WriteVariant8Tag(1);
            converter(obj, writer);
        }
    };
}

template <bool IsPySchemaOptional, bool IsTITypeOptional, typename TConverter>
TPythonToSkiffConverter WrapPythonToSkiffConverterImpl(TConverter converter)
{
    if constexpr (IsPySchemaOptional || IsTITypeOptional) {
        return CreateOptionalPythonToSkiffConverter<IsPySchemaOptional>(std::move(converter));
    } else {
        return converter;
    }
}

template <bool IsPySchemaOptional, typename TConverter>
TPythonToSkiffConverter MaybeWrapPythonToSkiffConverter(const Py::Object& pySchema, TConverter converter)
{
    if (IsTiTypeOptional(pySchema)) {
        return WrapPythonToSkiffConverterImpl<IsPySchemaOptional, true>(std::move(converter));
    } else {
        return WrapPythonToSkiffConverterImpl<IsPySchemaOptional, false>(std::move(converter));
    }
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
            THROW_ERROR_EXCEPTION("Got too large integer value %v",
                Repr(Py::Object(obj)))
                << Py::BuildErrorFromPythonException(/*clear*/ true);
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

template <bool IsPySchemaOptional>
TPythonToSkiffConverter CreatePrimitivePythonToSkiffConverterImpl(TString description, Py::Object pySchema, EPythonType pythonType)
{
    auto wireTypeStr = Py::ConvertStringObjectToString(GetAttr(pySchema, WireTypeFieldName));
    auto wireType = ::FromString<EWireType>(wireTypeStr);

    switch (pythonType) {
        case EPythonType::Int:
            switch (wireType) {
#define CASE(WireType) \
                case WireType: { \
                    auto converter = TPrimitivePythonToSkiffConverter<WireType, EPythonType::Int>(description); \
                    return MaybeWrapPythonToSkiffConverter<IsPySchemaOptional>( \
                        std::move(pySchema), \
                        std::move(converter)); \
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
                return MaybeWrapPythonToSkiffConverter<IsPySchemaOptional>( \
                    std::move(pySchema), \
                    std::move(converter)); \
            }

        CASE(EPythonType::Bytes, EWireType::String32)
        CASE(EPythonType::Str, EWireType::String32)
        CASE(EPythonType::Float, EWireType::Double)
        CASE(EPythonType::Bool, EWireType::Boolean)
#undef CASE
    }
    Y_FAIL();
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

template <bool IsPySchemaOptional>
TPythonToSkiffConverter CreatePrimitivePythonToSkiffConverter(TString description, Py::Object pySchema)
{
    auto middlewareTypeConverter = GetAttr(pySchema, "_to_yt_type");

    EPythonType pythonType;
    if (middlewareTypeConverter.isNone()) {
        pythonType = GetPythonType(GetAttr(pySchema, PyTypeFieldName));
    } else {
        pythonType = GetPythonType(GetAttr(pySchema, PyWireTypeFieldName));
    }

    auto primitiveConverter = CreatePrimitivePythonToSkiffConverterImpl<IsPySchemaOptional>(
        description,
        std::move(pySchema),
        pythonType);

    if (middlewareTypeConverter.isNone()) {
        return primitiveConverter;
    }
    return WrapWithMiddlewareConverter(std::move(primitiveConverter), Py::Callable(std::move(middlewareTypeConverter)));
}

class TStructPythonToSkiffConverter
{
public:
    TStructPythonToSkiffConverter(TString description, Py::Object pySchema)
        : Description_(description)
    {
        static auto StructFieldClass = GetSchemaType("StructField");
        auto fields = Py::List(GetAttr(pySchema, FieldsFieldName));
        for (const auto& field : fields) {
            if (PyObject_IsInstance(field.ptr(), StructFieldClass.get())) {
                auto fieldName = Py::ConvertStringObjectToString(GetAttr(field, NameFieldName));
                auto fieldDescription = Description_ + "." + fieldName;
                FieldConverters_.push_back(CreatePythonToSkiffConverter(
                    fieldDescription,
                    GetAttr(field, PySchemaFieldName)));
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
    explicit TListPythonToSkiffConverter(TString description, Py::Object pySchema)
        : Description_(description)
        , ItemConverter_(CreatePythonToSkiffConverter(
            Description_ + ".<list-element>",
            GetAttr(pySchema, ItemFieldName)))
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
    explicit TTuplePythonToSkiffConverter(TString description, Py::Object pySchema)
        : Description_(description)
    {
        int i = 0;
        for (const auto& pyElementSchema : Py::List(GetAttr(pySchema, ElementsFieldName))) {
            ElementConverters_.push_back(
                CreatePythonToSkiffConverter(Format("%v.<tuple-element-%v>", description, i), pyElementSchema)
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
    explicit TDictPythonToSkiffConverter(TString description, Py::Object pySchema)
        : Description_(description)
        , KeyConverter_(CreatePythonToSkiffConverter(
            Description_ + ".<key>",
            GetAttr(pySchema, KeyFieldName)))
        , ValueConverter_(CreatePythonToSkiffConverter(
            Description_ + ".<value>",
            GetAttr(pySchema, ValueFieldName)))
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
        , StructConverter_(RowClassName_, GetAttr(pySchema, StructSchemaFieldName))
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
    TStructPythonToSkiffConverter StructConverter_;
    std::optional<TString> OtherColumnsFieldName_;
};

TPythonToSkiffConverter CreateRowPythonToSkiffConverter(Py::Object pySchema)
{
    return TRowPythonToSkiffConverter(std::move(pySchema));
}

template <bool IsPySchemaOptional>
TPythonToSkiffConverter CreatePythonToSkiffConverterImpl(TString description, Py::Object pySchema)
{
    static auto StructSchemaClass = GetSchemaType("StructSchema");
    static auto PrimitiveSchemaClass = GetSchemaType("PrimitiveSchema");
    static auto OptionalSchemaClass = GetSchemaType("OptionalSchema");
    static auto ListSchemaClass = GetSchemaType("ListSchema");
    static auto TupleSchemaClass = GetSchemaType("TupleSchema");
    static auto DictSchemaClass = GetSchemaType("DictSchema");

    if (PyObject_IsInstance(pySchema.ptr(), StructSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter<IsPySchemaOptional>(
            pySchema,
            TStructPythonToSkiffConverter(description, pySchema));
    } else if (PyObject_IsInstance(pySchema.ptr(), PrimitiveSchemaClass.get())) {
        return CreatePrimitivePythonToSkiffConverter<IsPySchemaOptional>(description, std::move(pySchema));
    } else if (PyObject_IsInstance(pySchema.ptr(), OptionalSchemaClass.get())) {
        if constexpr (IsPySchemaOptional) {
            THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Optional[Optional[T]] is not allowed");
        }
        return CreatePythonToSkiffConverterImpl<true>(
            description + ".<optional-element>",
            GetAttr(pySchema, ItemFieldName));
    } else if (PyObject_IsInstance(pySchema.ptr(), ListSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter<IsPySchemaOptional>(
            pySchema,
            TListPythonToSkiffConverter(description, pySchema));
    } else if (PyObject_IsInstance(pySchema.ptr(), TupleSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter<IsPySchemaOptional>(
            pySchema,
            TTuplePythonToSkiffConverter(description, pySchema));
    } else if (PyObject_IsInstance(pySchema.ptr(), DictSchemaClass.get())) {
        return MaybeWrapPythonToSkiffConverter<IsPySchemaOptional>(
            pySchema,
            TDictPythonToSkiffConverter(description, pySchema));
    } else {
        THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Unknown schema type %Qv",
            Repr(pySchema.type()));
    }
}

TPythonToSkiffConverter CreatePythonToSkiffConverter(TString description, Py::Object pySchema)
{
    static auto OptionalSchemaClass = GetSchemaType("OptionalSchema");

    if (PyObject_IsInstance(pySchema.ptr(), OptionalSchemaClass.get())) {
        if (!IsTiTypeOptional(pySchema)) {
            THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Can not write optional "
                "python field %Qv to non-optional schema field",
                description);
        }
        return CreatePythonToSkiffConverterImpl<true>(description + ".<optional-element>", GetAttr(pySchema, ItemFieldName));
    } else {
        return CreatePythonToSkiffConverterImpl<false>(description, std::move(pySchema));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
