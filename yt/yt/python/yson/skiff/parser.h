#pragma once

#include "public.h"
#include "consumer.h"
#include "../rows_iterator_base.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/library/skiff_ext/parser.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffIterator
    : public TRowsIteratorBase<TSkiffIterator, TPythonSkiffRecordBuilder, NSkiffExt::TSkiffMultiTableParser<TPythonSkiffRecordBuilder>>
{
public:
    TSkiffIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    void Initialize(
        IInputStream* inputStream,
        std::unique_ptr<IInputStream> inputStreamHolder,
        const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffschemaList,
        const std::string& rangeIndexColumnName,
        const std::string& rowIndexColumnName,
        const std::optional<TString>& encoding);

    static void InitType();

    using TBase = TRowsIteratorBase<TSkiffIterator, TPythonSkiffRecordBuilder, NSkiffExt::TSkiffMultiTableParser<TPythonSkiffRecordBuilder>>;

private:
    static constexpr const char FormatName[] = "Skiff";

    std::unique_ptr<IInputStream> InputStreamHolder_;
};

////////////////////////////////////////////////////////////////////////////////

Py::Object LoadSkiff(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
