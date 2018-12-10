#include "parser.h"
#include "raw_iterator.h"
#include "parser_helpers.h"
#include "../helpers.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template<>
TString TSkiffIterator::TBase::Name_ = TString();
template<>
TString TSkiffIterator::TBase::Doc_ = TString();

TSkiffIterator::TSkiffIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : TBase::TRowsIteratorBase(self, args, kwargs, FormatName)
{ }

void TSkiffIterator::Initialize(
    IInputStream* inputStream,
    std::unique_ptr<IInputStream> inputStreamHolder,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffschemaList,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName,
    const std::optional<TString>& encoding)
{
    YCHECK(inputStreamHolder.get() == inputStream);
    InputStream_ = inputStream;
    Consumer_ = std::make_unique<TPythonSkiffRecordBuilder>(pythonSkiffschemaList, encoding);
    InputStreamHolder_ = std::move(inputStreamHolder);

    Parser_ = CreateSkiffMultiTableParser<TPythonSkiffRecordBuilder>(
        Consumer_.get(),
        pythonSkiffschemaList,
        rangeIndexColumnName,
        rowIndexColumnName);
}

void TSkiffIterator::InitType()
{
    TBase::InitType(FormatName);
}

constexpr const char TSkiffIterator::FormatName[];

////////////////////////////////////////////////////////////////////////////////

Py::Object LoadSkiff(Py::Tuple& args, Py::Dict& kwargs)
{
    auto streamArg = ExtractArgument(args, kwargs, "stream");
    auto inputStreamHolder = CreateInputStreamWrapper(streamArg);
    auto inputStream = inputStreamHolder.get();

    auto schemasArg = ExtractArgument(args, kwargs, "schemas");
    if (!schemasArg.isList()) {
        throw Py::TypeError("\"schemas\" should be a list");
    }
    auto schemas = Py::List(schemasArg);

    auto rowIndexColumnName = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "row_index_column_name"));
    auto rangeIndexColumnName = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "range_index_column_name"));

    bool raw = false;
    if (HasArgument(args, kwargs, "raw")) {
        auto arg = ExtractArgument(args, kwargs, "raw");
        raw = Py::Boolean(arg);
    }

    auto encoding = ParseEncodingArgument(args, kwargs);

    ValidateArgumentsEmpty(args, kwargs);

    std::vector<Py::PythonClassObject<TSkiffSchemaPython>> pythonSkiffSchemas;
    for (const auto& schema : schemas) {
        Py::PythonClassObject<TSkiffSchemaPython> schemaPythonObject(schema);
        pythonSkiffSchemas.push_back(schemaPythonObject);
    }

    if (raw) {
        Py::Callable classType(TSkiffRawIterator::type());
        Py::PythonClassObject<TSkiffRawIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

        auto *iter = pythonIter.getCxxObject();
        iter->Initialize(
            inputStream,
            std::move(inputStreamHolder),
            pythonSkiffSchemas,
            rangeIndexColumnName,
            rowIndexColumnName);
        return pythonIter;
    } else {
        Py::Callable classType(TSkiffIterator::type());
        Py::PythonClassObject<TSkiffIterator> pythonIter(classType.apply(Py::Tuple(), Py::Dict()));

        auto *iter = pythonIter.getCxxObject();
        iter->Initialize(
            inputStream,
            std::move(inputStreamHolder),
            pythonSkiffSchemas,
            rangeIndexColumnName,
            rowIndexColumnName,
            encoding);
        return pythonIter;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
