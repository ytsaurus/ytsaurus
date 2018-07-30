#pragma once

#include "public.h"
#include "consumer.h"
#include "../rows_iterator_base.h"

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <yt/core/skiff/parser.h>
#include <yt/core/skiff/skiff_schema.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>


namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffIterator
    : public TRowsIteratorBase<TSkiffIterator, TPythonSkiffRecordBuilder, NSkiff::TSkiffMultiTableParser<TPythonSkiffRecordBuilder>>
{
public:
    TSkiffIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    void Initialize(
        IInputStream* inputStream,
        std::unique_ptr<IInputStream> inputStreamHolder,
        const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffschemaList,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName,
        const TNullable<TString>& encoding);

    static void InitType();

private:
    using TBase = TRowsIteratorBase<TSkiffIterator, TPythonSkiffRecordBuilder, NSkiff::TSkiffMultiTableParser<TPythonSkiffRecordBuilder>>;

    static constexpr const char FormatName[] = "Skiff";

    std::unique_ptr<IInputStream> InputStreamHolder_;
};

////////////////////////////////////////////////////////////////////////////////

Py::Object LoadSkiff(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
