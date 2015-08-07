#pragma once

#include "serialize.h"
#include "stream.h"

#include <contrib/libs/pycxx/Extensions.hxx>

#include <ytlib/driver/driver.h>
#include <core/yson/consumer.h>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

class TDriverResponse
    : public Py::PythonClass<TDriverResponse>
{
public:
    TDriverResponse(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs);

    void SetResponse(TFuture<void> response);

    NYson::IYsonConsumer* GetResponseParametersConsumer();

    void OwnInputStream(std::unique_ptr<TInputStreamWrap>& inputStream);

    void OwnOutputStream(std::unique_ptr<TOutputStreamWrap>& outputStream);

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

    std::unique_ptr<TInputStreamWrap> InputStream_;
    std::unique_ptr<TOutputStreamWrap> OutputStream_;
    std::unique_ptr<NYTree::TPythonObjectBuilder> ResponseParametersBuilder_;
    std::unique_ptr<NYTree::TGilGuardedYsonConsumer> ResponseParametersConsumer_;

    Py::Object ResponseParameters_;
    bool ResponseParametersFinished_ = false;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
