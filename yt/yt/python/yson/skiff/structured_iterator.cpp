#include "structured_iterator.h"

#include "error.h"
#include "schema.h"

#include <exception>
#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NPython {

using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TIteratorWithContext
    : public Py::PythonClass<TIteratorWithContext>
{
public:
    TIteratorWithContext(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TIteratorWithContext>::PythonClass(self, args, kwargs)
    {
        if (args.size() != 1) {
            throw Py::TypeError("SkiffStructuredIteratorWithContext.__init__ takes 1 argument");
        }
        auto pyObject = Py::PythonClassObject<TSkiffStructuredIterator>(args[0]);
        Iterator_ = pyObject.getCxxObject();
        IteratorObject_ = pyObject;
    }

    Py::Object iter() override
    {
        return self();
    }

    PyObject* iternext() override
    {
        auto row = PyObjectPtr(Iterator_->iternext());
        if (!row) {
            return nullptr;
        }
        auto tuple = PyObjectPtr(PyTuple_New(2));
        if (PyTuple_SetItem(tuple.get(), 0, row.get()) != 0) {
            return nullptr;
        }
        Y_UNUSED(row.release());
        if (PyTuple_SetItem(tuple.get(), 1, Py::new_reference_to(IteratorObject_)) != 0) {
            return nullptr;
        }
        return tuple.release();
    }

    static void InitType()
    {
        behaviors().name("yt_yson_bindings.yson_lib.SkiffStructuredIteratorWithContext");
        behaviors().doc("Iterates over pairs (row, context)");
        behaviors().supportGetattro();
        behaviors().supportSetattro();
        behaviors().supportIter();

        behaviors().readyType();
    }

private:
    Py::Object IteratorObject_;
    TSkiffStructuredIterator* Iterator_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSkiffStructuredIterator::TSkiffStructuredIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TSkiffStructuredIterator>::PythonClass(self, args, kwargs)
{ }

void TSkiffStructuredIterator::Initialize(
    std::unique_ptr<IInputStream> inputStreamHolder,
    const Py::List& pySchemaList,
    TSkiffSchemaList skiffSchemaList,
    bool raw)
{
    Raw_ = raw;
    InputStreamHolder_ = std::move(inputStreamHolder);
    BufferedStream_ = std::make_unique<TBufferedInput>(InputStreamHolder_.get());

    IZeroCopyInput* parserInputStream;
    if (Raw_) {
        BufferedStreamRepeater_ = std::make_unique<TTeeInputStream>(BufferedStream_.get());
        parserInputStream = BufferedStreamRepeater_.get();
    } else {
        parserInputStream = BufferedStream_.get();
    }

    Parser_ = std::make_unique<TCheckedInDebugSkiffParser>(
        CreateVariant16Schema(std::move(skiffSchemaList)),
        parserInputStream);
    for (const auto& pySchema : pySchemaList) {
        Converters_.emplace_back(CreateRowSkiffToPythonConverter(pySchema));
    }
}

Py::Object TSkiffStructuredIterator::iter()
{
    return self();
}

PyObject* TSkiffStructuredIterator::iternext()
{
    if (!Parser_->HasMoreData()) {
        PyErr_SetNone(PyExc_StopIteration);
        return nullptr;
    }

    ++RowContext_.RowIndex;
    size_t numParsedBytes = Parser_->GetReadBytesCount();

    ui16 tableIndex;
    try {
        tableIndex = Parser_->ParseVariant16Tag();
        if (tableIndex >= Converters_.size()) {
            THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Skiff table index must be in range [0, %v), got %v",
                Converters_.size(),
                tableIndex);
        }
    } catch (const std::exception& exc) {
        throw CreateSkiffError("Failed to parse Skiff table index", TError(exc), &RowContext_);
    }

    RowContext_.TableIndex = tableIndex;

    try {
        PyObjectPtr obj = Converters_[tableIndex](Parser_.get(), &RowContext_);

        if (Raw_) {
            size_t curRowBytesSize = Parser_->GetReadBytesCount() - numParsedBytes;
            TmpBuffer_.Clear();
            BufferedStreamRepeater_->ExtractFromBuffer(&TmpBuffer_, curRowBytesSize);
            PyObject* bytes = PyBytes_FromStringAndSize(TmpBuffer_.data(), TmpBuffer_.size());
            return bytes;
        } else {
            return obj.release();
        }
    } catch (const TErrorException& exception) {
        throw CreateSkiffError(
            "Skiff to Python conversion failed",
            exception.Error(),
            &RowContext_);
    } catch (const std::exception& exception) {
        throw CreateSkiffError(
            "Skiff to Python conversion failed",
            TError(exception),
            &RowContext_);
    }
}

Py::Object TSkiffStructuredIterator::GetTableIndex() const
{
    return Py::Long(RowContext_.TableIndex);
}

Py::Object TSkiffStructuredIterator::GetKeySwitch() const
{
    return Py::Boolean(RowContext_.KeySwitch);
}

Py::Object TSkiffStructuredIterator::GetRowIndex() const
{
    if (RowContext_.RowIndex == ERowIndex::NotAvailable) {
        throw CreateSkiffError(
            "RowIndex requested, but it's not available. "
            "Possibly you're using dynamic tables, that doesn't support it",
            TError(),
            &RowContext_);
    }
    return Py::Long(RowContext_.RowIndex);
}

Py::Object TSkiffStructuredIterator::GetRangeIndex() const
{
    return Py::Long(RowContext_.RangeIndex);
}

Py::Object TSkiffStructuredIterator::WithContext()
{
    Py::Callable classType(TIteratorWithContext::type());
    auto tuple = Py::Tuple(1);
    tuple[0] = self();
    return classType.apply(tuple);
}

void TSkiffStructuredIterator::InitType()
{
    TIteratorWithContext::InitType();

    behaviors().name("yt_yson_bindings.yson_lib.SkiffStructuredIterator");
    behaviors().doc("Iterates over stream with skiff rows and returns their structured representation");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    PYCXX_ADD_NOARGS_METHOD(get_table_index, GetTableIndex, "Returns index of table current row belongs to");
    PYCXX_ADD_NOARGS_METHOD(get_key_switch, GetKeySwitch, "Returns true iff current row's key differs from previous one's");
    PYCXX_ADD_NOARGS_METHOD(get_row_index, GetRowIndex, "Returns index of current row");
    PYCXX_ADD_NOARGS_METHOD(get_range_index, GetRangeIndex, "Returns index of range current row belongs to");
    PYCXX_ADD_NOARGS_METHOD(with_context, WithContext, "Returns iterator over the pairs (context, row)");

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

Py::Object LoadSkiffStructured(Py::Tuple& args, Py::Dict& kwargs)
{
    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto inputStreamHolder = CreateInputStreamWrapper(streamArg);
    auto pySchemasArg = ExtractArgument(args, kwargs, "py_schemas");
    if (!pySchemasArg.isList()) {
        throw Py::TypeError("\"py_schemas\" should be a list");
    }
    auto pySchemas = Py::List(pySchemasArg);
    auto skiffSchemasArg = ExtractArgument(args, kwargs, "skiff_schemas");
    if (!skiffSchemasArg.isList()) {
        throw Py::TypeError("\"skiff_schemas\" should be a list");
    }
    auto skiffSchemasPython = Py::List(skiffSchemasArg);

    bool raw = false;
    if (HasArgument(args, kwargs, "raw")) {
        raw = ExtractArgument(args, kwargs, "raw").as_bool();
    }

    ValidateArgumentsEmpty(args, kwargs);

    TSkiffSchemaList skiffSchemas;
    for (const auto& schema : skiffSchemasPython) {
        Py::PythonClassObject<TSkiffSchemaPython> schemaPythonObject(schema);
        skiffSchemas.push_back(schemaPythonObject.getCxxObject()->GetSchemaObject()->GetSkiffSchema());
    }
    // TODO(aleexfi): Add TSkiffRawStructuredIterator to parse only system fields
    Py::Callable classType(TSkiffStructuredIterator::type());
    Py::PythonClassObject<TSkiffStructuredIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

    auto *iter = pythonIter.getCxxObject();
    iter->Initialize(
        std::move(inputStreamHolder),
        std::move(pySchemas),
        std::move(skiffSchemas),
        raw);
    return pythonIter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
