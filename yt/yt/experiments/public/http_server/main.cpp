#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/server.h>

using namespace NYT;
using namespace NYT::NHttp;
using namespace NYT::NConcurrency;


YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

class THandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        YT_LOG_INFO("Started reading request body");
        while (true) {
            auto chunk = WaitFor(req->Read())
                .ValueOrThrow();
            if (!chunk) {
                break;
            }
            YT_LOG_INFO("Chunk received (Size: %v)", chunk.size());
        }
        YT_LOG_INFO("Finished reading request body");

        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->Close())
            .ThrowOnError();
    }
};

int main()
{
    auto serverConfig = New<TServerConfig>();
    serverConfig->Port = 8000;

    auto server = CreateServer(serverConfig);
    server->AddHandler("/write", New<THandler>());
    server->Start();

    Sleep(TDuration::Max());
}
