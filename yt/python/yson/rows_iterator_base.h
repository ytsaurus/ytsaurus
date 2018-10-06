#pragma once

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <util/generic/string.h>
#include <util/generic/hash.h>


namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TIteratorClass, class TConsumer, class TParser>
class TRowsIteratorBase
    : public Py::PythonClass<TIteratorClass>
{
public:
    TRowsIteratorBase(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs, const TString& formatName);

    Py::Object iter();

    PyObject* iternext();

    virtual ~TRowsIteratorBase();

protected:
    static void InitType(const TString& formatName);

    static TString Name_;
    static TString Doc_;

    // These fields must be initialized in derived class.
    IInputStream* InputStream_;
    std::unique_ptr<TConsumer> Consumer_;
    std::unique_ptr<TParser> Parser_;

private:
    using TBase = Py::PythonClass<TIteratorClass>;

    const TString FormatName_;

    bool IsStreamExhausted_ = false;

    static constexpr int BufferSize = 1024 * 64;
    TBlob Buffer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

#define ROWS_ITERATOR_BASE_INL_H_
#include "rows_iterator_base-inl.h"
#undef ROWS_ITERATOR_BASE_INL_H_
