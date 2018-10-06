#pragma once
#ifndef ROWS_ITERATOR_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include rows_iterator_base.h"
#endif

#include "error.h"

#include <yt/core/misc/ref_counted_tracker.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

struct TRowsIteratorBufferTag { };

template <class TIteratorClass, class TConsumer, class TParser>
TRowsIteratorBase<TIteratorClass, TConsumer, TParser>::TRowsIteratorBase(
    Py::PythonClassInstance* self,
    Py::Tuple& args,
    Py::Dict& kwargs,
    const TString& formatName)
    : TBase::PythonClass(self, args, kwargs)
    , FormatName_(formatName)
    , Buffer_(TRowsIteratorBufferTag(), BufferSize, /* initializeStorage */ false)
{ }

template <class TIteratorClass, class TConsumer, class TParser>
Py::Object TRowsIteratorBase<TIteratorClass, TConsumer, TParser>::iter()
{
    return TBase::self();
}

template <class TIteratorClass, class TConsumer, class TParser>
PyObject* TRowsIteratorBase<TIteratorClass, TConsumer, TParser>::iternext()
{
    YCHECK(InputStream_);
    YCHECK(Consumer_);
    YCHECK(Parser_);

    try {
        // Read unless we have the whole row.
        while (!Consumer_->HasObject() && !IsStreamExhausted_) {
            size_t length = InputStream_->Read(Buffer_.Begin(), BufferSize);
            if (length != 0) {
                Parser_->Read(TStringBuf(Buffer_.Begin(), length));
            }
            if (length != BufferSize) {
                IsStreamExhausted_ = true;
                Parser_->Finish();
            }
        }

        // Stop iteration if we are done.
        if (!Consumer_->HasObject()) {
            PyErr_SetNone(PyExc_StopIteration);
            return nullptr;
        }

        auto result = Consumer_->ExtractObject();
        // We should return pointer to alive object.
        result.increment_reference_count();
        return result.ptr();
    } CATCH_AND_CREATE_YSON_ERROR(FormatName_ + " load failed");
}

template <class TIteratorClass, class TConsumer, class TParser>
TRowsIteratorBase<TIteratorClass, TConsumer, TParser>::~TRowsIteratorBase() = default;

template <class TIteratorClass, class TConsumer, class TParser>
void TRowsIteratorBase<TIteratorClass, TConsumer, TParser>::InitType(const TString& formatName)
{
    Name_ = formatName + " iterator";
    Doc_ = "Iterates over stream with " + formatName + " rows";
    TBase::behaviors().name(Name_.c_str());
    TBase::behaviors().doc(Doc_.c_str());
    TBase::behaviors().supportGetattro();
    TBase::behaviors().supportSetattro();
    TBase::behaviors().supportIter();

    TBase::behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
