#include "driver.h"

#include "private.h"
#include "client.h"
#include "commands.h"
#include "protocol.h"
#include "session.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/cast.h>

#include <util/generic/cast.h>

namespace NYT::NZookeeper {

using namespace NConcurrency;

static const auto& Logger = ZookeeperLogger;

////////////////////////////////////////////////////////////////////////////////

struct TRequestHeader
{
    int Xid = -1;
    ECommandType CommandType = ECommandType::None;

    TGuid RequestId;

    void Deserialize(IZookeeperProtocolReader* reader)
    {
        Xid = reader->ReadInt();
        CommandType = CheckedEnumCast<ECommandType>(reader->ReadInt());
        RequestId = TGuid::Create();
    }
};

struct TResponseHeader
{
    int Xid = -1;
    i64 Zxid = -1;
    EZookeeperError Error = EZookeeperError::OK;

    void Serialize(IZookeeperProtocolWriter* writer)
    {
        writer->WriteInt(Xid);
        writer->WriteLong(Zxid);
        writer->WriteInt(ToUnderlying(Error));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriver
    : public IDriver
{
public:
    explicit TDriver(IClientPtr client)
        : Client_(std::move(client))
    { }

    TSharedRef ExecuteRequest(ISessionPtr session, TSharedRef request) noexcept override
    {
        auto reader = CreateZookeeperProtocolReader(std::move(request));

        TRequestHeader header;

        try {
            header.Deserialize(reader.get());
        } catch (const std::exception& ex) {
            auto error = TError("Failed to parse request header") << ex;
            YT_LOG_DEBUG(error, "Failed to execute request "
                "(SessionId: %v, Xid: %v, RequestType: %v)",
                session->GetId(),
                header.Xid,
                header.CommandType);

            return MakeErrorResponse(error, header);
        }

        try {
            auto writer = CreateZookeeperProtocolWriter();
            GuardedExecuteRequest(session, header, reader.get(), writer.get());
            return writer->Finish();
        } catch (const std::exception& ex) {
            auto error = TError("Failed to execute request") << ex;
            YT_LOG_DEBUG(error, "Failed to execute request "
                "(SessionId: %v, Xid: %v, RequestType: %v)",
                session->GetId(),
                header.Xid,
                header.CommandType);

            return MakeErrorResponse(error, header);
        }
    }

private:
    const IClientPtr Client_;

    TSharedRef MakeErrorResponse(const TError& /*error*/, const TRequestHeader& request)
    {
        TResponseHeader response{
            .Xid = request.Xid,
            .Zxid = 0,
            .Error = EZookeeperError::ApiError
        };

        auto writer = CreateZookeeperProtocolWriter();
        response.Serialize(writer.get());
        return writer->Finish();
    }

    void WriteResponseHeader(
        IZookeeperProtocolWriter* response,
        const TRequestHeader& requestHeader,
        i64 zxid)
    {
        TResponseHeader responseHeader{
            .Xid = requestHeader.Xid,
            .Zxid = zxid,
            .Error = EZookeeperError::OK,
        };
        responseHeader.Serialize(response);
    }

    void GuardedExecuteRequest(
        const ISessionPtr& session,
        const TRequestHeader& header,
        IZookeeperProtocolReader* request,
        IZookeeperProtocolWriter* response)
    {
        auto traceContext = NTracing::TTraceContext::NewRoot("Zookeeper");
        NTracing::TTraceContextGuard guard(traceContext);

        NProfiling::TWallTimer timer;

        YT_LOG_DEBUG("Started executing command "
            "(SessionId: %v, Xid: %v, RequestId: %v, CommandType: %v)",
            session->GetId(),
            header.Xid,
            header.RequestId,
            header.CommandType);

        switch (header.CommandType) {
            case ECommandType::Ping:
                ExecutePing(session, header, request, response);
                break;
            case ECommandType::GetChildren2:
                ExecuteGetChildren2(session, header, request, response);
                break;
            default:
                YT_ABORT();
        }

        NTracing::FlushCurrentTraceContextTime();

        YT_LOG_DEBUG("Finished executing command "
            "(SessionId: %v, Xid: %v, CommandType: %v, WallTime: %v, CpuTime: %v)",
            session->GetId(),
            header.Xid,
            header.CommandType,
            timer.GetElapsedTime(),
            traceContext->GetElapsedTime());
    }

#define DEFINE_COMMAND(command_name) \
    void Execute ## command_name( \
        const ISessionPtr& session, \
        const TRequestHeader& header, \
        IZookeeperProtocolReader* request, \
        IZookeeperProtocolWriter* response) \
    { \
        auto req = New<TReq ## command_name>(); \
        req->Deserialize(request); \
        request->ValidateFinished(); \
        auto rsp = Client_->command_name(GetRequestContext(std::move(req), header, session)); \
        WriteResponseHeader(response, header, rsp->Zxid); \
        rsp->Serialize(response); \
    }

    DEFINE_COMMAND(Ping)
    DEFINE_COMMAND(GetChildren2)

    template <typename TRequestPtr>
    TRequestContext<TRequestPtr> GetRequestContext(
        TRequestPtr request,
        const TRequestHeader& header,
        ISessionPtr session)
    {
        return TRequestContext<TRequestPtr>{
            .Request = std::move(request),
            .RequestId = header.RequestId,
            .Session = std::move(session),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(IClientPtr client)
{
    return New<TDriver>(std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
