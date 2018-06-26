#pragma once

#include <yt/python/common/stream.h>

#include <yt/python/yson/object_builder.h>
#include <yt/python/yson/serialize.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/core/yson/consumer.h>

#include <pycxx/Extensions.hxx>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TDriverResponseHolder
    : public TIntrinsicRefCounted
{
public:
    TDriverResponseHolder();
    virtual ~TDriverResponseHolder();

    NYson::IYsonConsumer* GetResponseParametersConsumer();
    NYTree::TPythonObjectBuilder* GetPythonObjectBuilder();

    void OwnInputStream(std::unique_ptr<TInputStreamWrap>& inputStream);
    void OwnOutputStream(std::unique_ptr<TOutputStreamWrap>& outputStream);
private:
    std::unique_ptr<TInputStreamWrap> InputStream_;
    std::unique_ptr<TOutputStreamWrap> OutputStream_;
    std::unique_ptr<NYTree::TPythonObjectBuilder> ResponseParametersBuilder_;
    std::unique_ptr<NYTree::TGilGuardedYsonConsumer> ResponseParametersConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

class TDriverResponse
    : public Py::PythonClass<TDriverResponse>
{
public:
    TDriverResponse(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs);

    void SetResponse(TFuture<void> response);
    TIntrusivePtr<TDriverResponseHolder> GetHolder() const;

    Py::Object ResponseParameters(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, ResponseParameters);

    Py::Object Wait(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, Wait);

    Py::Object IsSet(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, IsSet);

    Py::Object IsOk(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, IsOk);

    Py::Object Error(Py::Tuple& args, Py::Dict& kwargs);
    PYCXX_KEYWORDS_METHOD_DECL(TDriverResponse, Error);

    virtual ~TDriverResponse();

    static void InitType();

private:
    TFuture<void> Response_;
    TIntrusivePtr<TDriverResponseHolder> Holder_;

    Py::Object ResponseParameters_;
    bool ResponseParametersFinished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
