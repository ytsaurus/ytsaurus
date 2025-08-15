#pragma once

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/python/yson/rows_iterator_base.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/fwd.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/writer.h>

#include <queue>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFileFormat,
    (Parquet)
    (Orc)
);

class TArrowOutputStream
    : public arrow20::io::OutputStream
{
public:
    arrow20::Status Write(const void* data, int64_t nbytes) override;

    arrow20::Status Flush() override;

    arrow20::Status Close() override;

    arrow20::Result<int64_t> Tell() const override;

    bool closed() const override;

    bool IsEmpty() const;

    PyObject* Get();

private:
    i64 Position_ = 0;
    std::queue<TString> Data_;
    bool IsClosed_ = false;
    i64 DataWeight_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TArrowRawIterator
    : public Py::PythonClass<TArrowRawIterator>
{
public:
    TArrowRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    void Initialize(const TString& inputFilePath, EFileFormat format, int arrowBatchSize);

    Py::Object iter() override;
    PyObject* iternext() override;

    static void InitType();

    Py::Object GetSchema(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TArrowRawIterator, GetSchema)

    Py::Object NextChunk(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TArrowRawIterator, NextChunk)

private:
    std::shared_ptr<arrow20::ipc::RecordBatchWriter> RecordBatchWriter_;
    std::shared_ptr<arrow20::RecordBatchReader> RecordBatchReader_;
    std::unique_ptr<parquet20::arrow20::FileReader> ArrowFileReader_;
    TArrowOutputStream Pipe_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
