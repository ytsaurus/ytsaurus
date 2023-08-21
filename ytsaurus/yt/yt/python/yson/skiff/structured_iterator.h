#pragma once

#include "public.h"

#include "converter_skiff_to_python.h"

#include <yt/yt/python/common/tee_input_stream.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <util/stream/buffered.h>


namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TSkiffStructuredIterator
    : public Py::PythonClass<TSkiffStructuredIterator>
{
public:
    TSkiffStructuredIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    void Initialize(
        std::unique_ptr<IInputStream> inputStreamHolder,
        const Py::List& pySchemaList,
        NSkiff::TSkiffSchemaList skiffSchemaList,
        bool raw);

    Py::Object iter() override;
    PyObject* iternext() override;

    static void InitType();

private:
    std::unique_ptr<IInputStream> InputStreamHolder_;
    std::unique_ptr<TBufferedInput> BufferedStream_;
    std::unique_ptr<TTeeInputStream> BufferedStreamRepeater_;
    TBuffer TmpBuffer_;

    std::unique_ptr<NSkiff::TCheckedInDebugSkiffParser> Parser_;
    std::vector<TRowSkiffToPythonConverter> Converters_;
    TSkiffRowContext RowContext_;
    bool Raw_ = false;

private:
    Py::Object GetTableIndex() const;
    PYCXX_NOARGS_METHOD_DECL(TSkiffStructuredIterator, GetTableIndex)

    Py::Object GetKeySwitch() const;
    PYCXX_NOARGS_METHOD_DECL(TSkiffStructuredIterator, GetKeySwitch)

    Py::Object GetRowIndex() const;
    PYCXX_NOARGS_METHOD_DECL(TSkiffStructuredIterator, GetRowIndex)

    Py::Object GetRangeIndex() const;
    PYCXX_NOARGS_METHOD_DECL(TSkiffStructuredIterator, GetRangeIndex)

    // This method must be const, but is not due to bug in PyCXX (self() method is not const).
    Py::Object WithContext();
    PYCXX_NOARGS_METHOD_DECL(TSkiffStructuredIterator, WithContext)
};

Py::Object LoadSkiffStructured(Py::Tuple& args, Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
