#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/companion/server/companion_main.h>
#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

namespace NYT::NFlow::NCompanionTest {

////////////////////////////////////////////////////////////////////////////////

struct THttpGetParameters
    : public NYTree::TYsonStruct
{
    std::string Url;
    bool UseHttpsClient{};

    REGISTER_YSON_STRUCT(THttpGetParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("url", &TThis::Url)
            .NonEmpty();
        registrar.Parameter("use_https_client", &TThis::UseHttpsClient)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

class THttpGetFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        auto parameters = initContext->GetParameters<THttpGetParameters>();
        Client_ = parameters->UseHttpsClient
            ? initContext->GetHttpsClient()
            : initContext->GetHttpClient();
        Url_ = parameters->Url;
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto response = NConcurrency::WaitFor(Client_->Get(Url_))
            .ValueOrThrow();

        auto builder = context->MakeOutputMessageBuilder("responses");
        builder.Payload().Set<std::string>(GetColumnValue<std::string>(message, "key"), "key");
        builder.Payload().Set<ui64>(static_cast<ui64>(response->GetStatusCode()), "status_code");
        output->AddMessage(builder.Finish());
    }

private:
    NHttp::IClientPtr Client_;
    std::string Url_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionTest

int main(int argc, const char** argv)
{
    NYT::NFlow::NCompanionServer::TPipeline pipeline;
    pipeline.AddTransform<
        NYT::NFlow::NCompanionTest::THttpGetFunction,
        NYT::NFlow::NCompanionTest::THttpGetParameters>("http-get");
    return NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline));
}
