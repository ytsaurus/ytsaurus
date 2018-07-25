#pragma once

#include "raw_consumer.h"
#include "public.h"
#include "../rows_iterator_base.h"

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <yt/core/skiff/parser.h>
#include <yt/core/skiff/skiff_schema.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <queue>


namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffRawIterator
    : public Py::PythonClass<TSkiffRawIterator>
{
public:
    TSkiffRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    void Initialize(
        IInputStream* inputStream,
        std::unique_ptr<IInputStream> inputStreamHolder,
        const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffschemaList,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName);

    Py::Object iter();
    PyObject* iternext();

    static void InitType();

    void ExtractRow();

private:
    TStreamReader InputStream_;

    std::unique_ptr<TPythonSkiffRawRecordBuilder> Consumer_;
    std::unique_ptr<NSkiff::TSkiffMultiTableParser<TPythonSkiffRawRecordBuilder>> Parser_;

    std::unique_ptr<IInputStream> InputStreamHolder_;
    bool IsStreamFinished_ = false;

    std::queue<TSharedRef> Rows_;

    ui64 ExtractedPrefixLength = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
