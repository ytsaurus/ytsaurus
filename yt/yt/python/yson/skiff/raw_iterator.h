#pragma once

#include "raw_consumer.h"
#include "public.h"
#include "../rows_iterator_base.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/library/skiff_ext/parser.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <queue>

namespace NYT::NPython {

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
        const std::string& rangeIndexColumnName,
        const std::string& rowIndexColumnName);

    Py::Object iter() override;
    PyObject* iternext() override;

    static void InitType();

    void ExtractRow();

private:
    TStreamReader InputStream_;

    std::unique_ptr<TPythonSkiffRawRecordBuilder> Consumer_;
    std::unique_ptr<NSkiffExt::TSkiffMultiTableParser<TPythonSkiffRawRecordBuilder>> Parser_;

    std::unique_ptr<IInputStream> InputStreamHolder_;
    bool IsStreamFinished_ = false;

    std::queue<TSharedRef> Rows_;

    ui64 ExtractedPrefixLength = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
