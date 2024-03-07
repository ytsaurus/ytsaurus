#pragma once

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <yt/yt/python/yson/rows_iterator_base.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <util/generic/fwd.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <queue>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TArrowOutputStream
    : public arrow::io::OutputStream
{
public:
    arrow::Status Write(const void* data, int64_t nbytes) override; // Use int64_t, because of DEVTOOLSSUPPORT-41558.

    arrow::Status Flush() override;

    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override; // Use int64_t, because of DEVTOOLSSUPPORT-41558.

    bool closed() const override;

    bool IsEmpty() const;

    PyObject* Get();

private:
    i64 Position_ = 0;
    std::queue<TString> Data_;
    bool IsClosed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TArrowRawIterator
    : public Py::PythonClass<TArrowRawIterator>
{
public:
    TArrowRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs);

    void Initialize(const TString& pathToFile);

    Py::Object iter() override;
    PyObject* iternext() override;

    static void InitType();

private:
    std::shared_ptr<arrow::ipc::RecordBatchWriter> RecordBatchWriter_;
    std::shared_ptr<arrow::RecordBatchReader> RecordBatchReader_;
    std::unique_ptr<parquet::arrow::FileReader> ArrowFileReader_;
    TArrowOutputStream Pipe_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
