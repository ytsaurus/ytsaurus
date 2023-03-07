#pragma once

#include <yt/python/common/stream.h>

#include <yt/python/yson/object_builder.h>
#include <yt/python/yson/serialize.h>

#include <yt/client/driver/driver.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/misc/blob_output.h>

#include <Extensions.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TDriverResponseHolder
    : public TIntrinsicRefCounted
{
public:
    TDriverResponseHolder();
    virtual ~TDriverResponseHolder();

    void Initialize();

    NYson::IFlushableYsonConsumer* GetResponseParametersConsumer() const;
    NYson::TYsonString GetResponseParametersYsonString() const;
    bool IsResponseParametersFinished() const;
    void OnResponseParametersFinished();

    void HoldInputStream(std::unique_ptr<IInputStream> inputStream);
    void HoldOutputStream(std::unique_ptr<IOutputStream>& outputStream);

    static void OnBeforePythonFinalize();
    static void OnAfterPythonFinalize();

private:
    std::atomic<bool> Initialized_ = {false};

    std::unique_ptr<IInputStream> InputStream_;
    std::unique_ptr<IOutputStream> OutputStream_;
    TBlobOutput ResponseParametersBlobOutput_;
    std::unique_ptr<NYson::IFlushableYsonConsumer> ResponseParametersYsonWriter_;
    std::atomic<bool> ResponseParametersFinished_ = {false};

    static TSpinLock DestructionSpinLock_;
    static std::atomic<bool> ShuttingDown_;
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

    static void InitType(const TString& moduleName);

private:
    TFuture<void> Response_;
    TIntrusivePtr<TDriverResponseHolder> Holder_;

    static TString TypeName_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
