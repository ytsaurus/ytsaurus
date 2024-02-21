#include "server.h"

#include "block_device.h"
#include "config.h"
#include "profiler.h"
#include "protocol.h"

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/system/byteorder.h>

namespace NYT::NNbd {

using namespace NConcurrency;
using namespace NNet;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNbdServer)

struct TNbdNetworkBufferTag
{ };

class TNbdServer
    : public INbdServer
{
public:
    TNbdServer(
        TNbdServerConfigPtr config,
        NApi::NNative::IConnectionPtr connection,
        IPollerPtr poller,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Connection_(std::move(connection))
        , Poller_(std::move(poller))
        , Invoker_(std::move(invoker))
    {
        ++NbdServerCount_;
        TNbdProfilerCounters::Get()->GetGauge({}, "/server/count").Update(NbdServerCount_);
        TNbdProfilerCounters::Get()->GetCounter({}, "/server/created").Increment(1);
    }

    ~TNbdServer()
    {
        --NbdServerCount_;
        TNbdProfilerCounters::Get()->GetGauge({}, "/server/count").Update(NbdServerCount_);
        TNbdProfilerCounters::Get()->GetCounter({}, "/server/removed").Increment(1);
    }

    void Start()
    {
        YT_LOG_INFO("Starting NBD server");

        try {
            int maxBacklogSize = 0;
            TNetworkAddress address;

            if (Config_->UnixDomainSocket) {
                maxBacklogSize = Config_->UnixDomainSocket->MaxBacklogSize;
                address = TNetworkAddress::CreateUnixDomainSocketAddress(Config_->UnixDomainSocket->Path);

                // Delete unix domain socket prior to binding.
                if (unlink(Config_->UnixDomainSocket->Path.c_str()) == -1 && LastSystemError() != ENOENT) {
                    THROW_ERROR_EXCEPTION(
                        "Failed to remove unix domain socket %v",
                        address)
                        << TError::FromSystem();
                }
            } else if (Config_->InternetDomainSocket) {
                maxBacklogSize = Config_->InternetDomainSocket->MaxBacklogSize;
                address = TNetworkAddress::CreateIPv6Any(Config_->InternetDomainSocket->Port);
            } else {
                THROW_ERROR_EXCEPTION("NBD server config must contain socket section");
            }

            YT_LOG_INFO("Creating listener (Address: %v)", address);

            Listener_ = CreateListener(
                address,
                Poller_,
                Poller_,
                maxBacklogSize);

            YT_LOG_INFO("Created listener (Address: %v)", address);

            AcceptConnection();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to start NBD server");
            throw;
        }

        YT_LOG_INFO("Started NBD server");
    }

    void RegisterDevice(
        const TString& name,
        IBlockDevicePtr device) override
    {
        YT_LOG_INFO("Registering device (Name: %v, Info: %v)", name, device->DebugString());

        auto guard = WriterGuard(NameToDeviceLock_);
        auto [it, inserted] = NameToDevice_.emplace(name, device);
        if (!inserted) {
            THROW_ERROR_EXCEPTION("Device %Qv with %Qv is already registered", name, device->DebugString());
        }

        TNbdProfilerCounters::Get()->GetCounter(TNbdProfilerCounters::MakeTagSet(device->GetProfileSensorTag()), "/device/registered").Increment(1);

        YT_LOG_INFO("Registered device (Name: %v, Info: %v)", name, device->DebugString());
    }

    virtual bool TryUnregisterDevice(const TString& name) override
    {
        YT_LOG_INFO("Unregistering device (Name: %v)", name);

        auto guard = WriterGuard(NameToDeviceLock_);
        auto it = NameToDevice_.find(name);
        if (it == NameToDevice_.end()) {
            YT_LOG_INFO("Can not unregister unknown device (Name: %v)", name);
            return false;
        }

        TNbdProfilerCounters::Get()->GetCounter(TNbdProfilerCounters::MakeTagSet(it->second->GetProfileSensorTag()), "/device/unregistered").Increment(1);

        NameToDevice_.erase(it);

        YT_LOG_INFO("Unregistered device (Name: %v)", name);
        return true;
    }

    virtual bool IsDeviceRegistered(const TString& name) const override
    {
        auto guard = ReaderGuard(NameToDeviceLock_);
        return NameToDevice_.contains(name);
    }

    virtual const NLogging::TLogger& GetLogger() const override
    {
        return Logger;
    }

    virtual NApi::NNative::IConnectionPtr GetConnection() const override
    {
        return Connection_;
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Invoker_;
    }

    const TNbdServerConfigPtr& GetConfig() const
    {
        return Config_;
    }

private:
    static std::atomic<int> NbdServerCount_;

    const NLogging::TLogger Logger = NbdLogger
        .WithTag("ServerId: %v", TGuid::Create());

    const TNbdServerConfigPtr Config_;
    const NApi::NNative::IConnectionPtr Connection_;
    const IPollerPtr Poller_;
    const IInvokerPtr Invoker_;

    IListenerPtr Listener_;

    mutable YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, NameToDeviceLock_);
    THashMap<TString, IBlockDevicePtr> NameToDevice_;


    std::vector<std::pair<TString, IBlockDevicePtr>> ListDevices()
    {
        auto guard = ReaderGuard(NameToDeviceLock_);
        return {NameToDevice_.begin(), NameToDevice_.end()};
    }

    IBlockDevicePtr FindDevice(const TString& name)
    {
        auto guard = ReaderGuard(NameToDeviceLock_);
        return GetOrDefault(NameToDevice_, name);
    }

    IBlockDevicePtr GetDeviceOrThrow(const TString& name)
    {
        auto device = FindDevice(name);
        if (!device) {
            THROW_ERROR_EXCEPTION("No such device %Qv",
                name);
        }
        return device;
    }


    class TConnectionHandler
        : public TRefCounted
    {
    public:
        TConnectionHandler(
            TNbdServerPtr server,
            IConnectionPtr connection)
            : Server_(std::move(server))
            , Connection_(std::move(connection))
            , Logger(Server_->GetLogger().WithTag("ConnectionId: %v", TGuid::Create()))
            , ResponseInvoker_(CreateBoundedConcurrencyInvoker(Server_->GetInvoker(), /*maxConcurrentInvocations*/ 1))
        { }

        void Run()
        {
            YT_UNUSED_FUTURE(BIND(&TConnectionHandler::FiberMain, MakeStrong(this))
                .AsyncVia(Server_->GetInvoker())
                .Run());
        }

    private:
        const TNbdServerPtr Server_;
        const IConnectionPtr Connection_;

        NLogging::TLogger Logger;
        const IInvokerPtr ResponseInvoker_;

        IBlockDevicePtr Device_;
        std::atomic<bool> Abort_ = false;


        void FiberMain()
        {
            YT_LOG_INFO("Connection accepted (RemoteAddress: %v)",
                Connection_->RemoteAddress());

            try {
                DoHandshake();
                if (Abort_) {
                    return;
                }
                DoTransmission();
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Connection failed");
            }
        }

        void DoHandshake()
        {
            YT_LOG_INFO("Handshake phase entered");

            {
                TServerHandshakeMessage message{
                    .Magic1 = HostToInet(TServerHandshakeMessage::ExpectedHostMagic1),
                    .Magic2 = HostToInet(TServerHandshakeMessage::ExpectedHostMagic2),
                    .Flags = HostToInet(EServerHandshakeFlags::NBD_FLAG_FIXED_NEWSTYLE),
                };
                WritePod(message);
            }

            {
                auto netFlags = ReadPod<EClientHandshakeFlags>();
                auto flags = InetToHost(netFlags);
                YT_LOG_INFO("Received client flags (Flags: %x)",
                    flags);
                if (flags != EClientHandshakeFlags::NBD_FLAG_C_FIXED_NEWSTYLE) {
                    THROW_ERROR_EXCEPTION("Unsupported client flags")
                        << TErrorAttribute("flags", flags);
                }
            }

            while (!Device_ && !Abort_) {
                auto message = ReadPod<TClientOptionMessage>();

                auto magic = InetToHost(message.Magic);
                if (magic != TClientOptionMessage::ExpectedHostMagic) {
                    THROW_ERROR_EXCEPTION("Invalid client option magic")
                        << TErrorAttribute("expected_magic", TClientOptionMessage::ExpectedHostMagic)
                        << TErrorAttribute("actual_magic", magic);
                }

                auto length = InetToHost(message.Length);
                if (length > TClientOptionMessage::MaxLength) {
                    THROW_ERROR_EXCEPTION("Client option is too long")
                        << TErrorAttribute("max_length", TClientOptionMessage::MaxLength)
                        << TErrorAttribute("actual_length", length);
                }

                auto option = InetToHost(message.Option);
                auto payload = ReadBuffer(length);
                HandleClientOption(option, payload);
            }
        }

        void DoTransmission()
        {
            YT_LOG_INFO("Transmission phase entered");

            while (!Abort_) {
                auto message = ReadPod<TClientRequestMessage>();

                auto magic = InetToHost(message.Magic);
                if (magic != TClientRequestMessage::ExpectedHostMagic) {
                    THROW_ERROR_EXCEPTION("Invalid client request magic")
                        << TErrorAttribute("expected_magic", TClientRequestMessage::ExpectedHostMagic)
                        << TErrorAttribute("actual_magic", magic);
                }

                auto length = InetToHost(message.Length);
                if (length > TClientRequestMessage::MaxLength) {
                    THROW_ERROR_EXCEPTION("Client request is too long")
                        << TErrorAttribute("max_length", TClientRequestMessage::MaxLength)
                        << TErrorAttribute("actual_length", length);
                }

                HandleClientRequest(message);
            }
        }

        void HandleClientOption(EClientOption option, const TSharedRef& payload)
        {
            switch (option) {
                case EClientOption::NBD_OPT_ABORT:
                    HandleAbortOption(payload);
                    break;

                case EClientOption::NBD_OPT_LIST:
                    HandleListOption(payload);
                    break;

                case EClientOption::NBD_OPT_EXPORT_NAME:
                    HandleExportNameOption(payload);
                    break;

                default:
                    YT_LOG_INFO("Received unknown client option (Option: %v, PayloadLength: %v)",
                        option,
                        payload.size());
                    WriteOptionResponse(option, EServerOptionReply::NBD_REP_ERR_UNSUP);
                    break;
            }
        }

        void HandleAbortOption(const TSharedRef& payload)
        {
            YT_LOG_INFO("Received NBD_OPT_ABORT client option, closing connection");

            if (!payload.empty()) {
                WriteOptionErrorResponseOnNonemptyPayload(EClientOption::NBD_OPT_ABORT);
                return;
            }

            WriteOptionResponse(EClientOption::NBD_OPT_ABORT, EServerOptionReply::NBD_REP_ACK);

            Abort_ = true;
        }

        void HandleListOption(const TSharedRef& payload)
        {
            YT_LOG_INFO("Received NBD_OPT_LIST client option");

            if (!payload.empty()) {
                WriteOptionErrorResponseOnNonemptyPayload(EClientOption::NBD_OPT_LIST);
                return;
            }

            for (const auto& [name, device] : Server_->ListDevices()) {
                WriteOptionResponse(EClientOption::NBD_OPT_LIST, EServerOptionReply::NBD_REP_SERVER, sizeof(ui32) + name.size());
                WritePod(HostToInet<ui32>(name.size()));
                WriteBuffer(TSharedRef::FromString(name));
            }

            WriteOptionResponse(EClientOption::NBD_OPT_LIST, EServerOptionReply::NBD_REP_ACK);
        }

        void HandleExportNameOption(const TSharedRef& payload)
        {
            auto name = ToString(payload);

            YT_LOG_INFO("Received NBD_OPT_EXPORT_NAME client option (Name: %v)",
                name);

            Device_ = Server_->GetDeviceOrThrow(name);

            Logger = Logger.WithTag("DeviceName: %v", name);

            auto flags =
                ETransmissionFlags::NBD_FLAG_HAS_FLAGS |
                ETransmissionFlags::NBD_FLAG_SEND_FLUSH |
                ETransmissionFlags::NBD_FLAG_SEND_FUA;
            if (Device_->IsReadOnly()) {
                flags |= ETransmissionFlags::NBD_FLAG_READ_ONLY;
                flags |= ETransmissionFlags::NBD_FLAG_CAN_MULTI_CONN;
            }

            TServerExportNameMessage message{
                .Size = HostToInet<ui64>(Device_->GetTotalSize()),
                .Flags = HostToInet(flags),
            };
            WritePod(message);
        }

        void WriteOptionErrorResponseOnNonemptyPayload(EClientOption option)
        {
            YT_LOG_WARNING("Unexpected payload in client option (Option: %v)",
                option);
            WriteOptionResponse(option, EServerOptionReply::NBD_REP_ERR_INVALID);
        }

        void WriteOptionResponse(EClientOption option, EServerOptionReply reply, ui32 length = 0)
        {
            TServerOptionMessage message{
                .Magic = HostToInet(TServerOptionMessage::ExpectedHostMagic),
                .Option = HostToInet(option),
                .Reply = HostToInet(reply),
                .Length = HostToInet<ui32>(length),
            };
            WritePod(message);
        }

        void HandleClientRequest(const TClientRequestMessage& message)
        {
            auto type = InetToHost(message.Type);
            auto cookie = InetToHost(message.Cookie);
            switch (type) {
                case ECommandType::NBD_CMD_READ:
                    HandleClientReadRequest(message);
                    break;

                case ECommandType::NBD_CMD_WRITE:
                    HandleClientWriteRequest(message);
                    break;

                case ECommandType::NBD_CMD_FLUSH:
                    HandleClientFlushRequest(message);
                    break;

                case ECommandType::NBD_CMD_DISC:
                    HandleClientDisconnectRequest(message);
                    break;

                default:
                    YT_LOG_DEBUG("Received unknown client message (Type: %v, Cookie: %x)",
                        type,
                        cookie);
                    WriteServerResponse(EServerError::NBD_EINVAL, cookie);
                    break;
            }
        }

        void HandleClientReadRequest(const TClientRequestMessage& message)
        {
            auto flags = InetToHost(message.Flags);
            auto cookie = InetToHost(message.Cookie);
            auto offset = InetToHost(message.Offset);
            auto length = InetToHost(message.Length);

            if (Server_->GetConfig()->TestAbortConnectionOnRead) {
                YT_LOG_DEBUG("Aborting connection for testing purposes on NBD_CMD_READ request (Cookie: %x, Offset: %v, Length: %v, Flags: %v)",
                    cookie,
                    offset,
                    length,
                    flags);

                Abort_ = true;
                return;
            }

            if (offset + length > static_cast<ui64>(Device_->GetTotalSize())) {
                YT_LOG_WARNING("Received an out-of-range NBD_CMD_READ request (Offset: %v, Length: %v, Size: %v)",
                    offset,
                    length,
                    Device_->GetTotalSize());
                WriteServerResponse(EServerError::NBD_EINVAL, cookie);
                return;
            }

            YT_LOG_DEBUG("Started serving NBD_CMD_READ request (Cookie: %x, Offset: %v, Length: %v, Flags: %v)",
                cookie,
                offset,
                length,
                flags);

            Device_->Read(offset, length)
                .Subscribe(
                    BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TSharedRef>& result) {
                        if (!result.IsOK()) {
                            YT_LOG_WARNING(result, "NBD_CMD_READ request failed (Cookie: %x)",
                                cookie);
                            WriteServerResponse(EServerError::NBD_EIO, cookie);
                            return;
                        }

                        YT_LOG_DEBUG("Finished serving NBD_CMD_READ request (Cookie: %x)",
                            cookie);

                        const auto& payload = result.Value();
                        YT_VERIFY(payload.size() == length);
                        WriteServerResponse(EServerError::NBD_OK, cookie, payload);
                    }));
        }

        void HandleClientWriteRequest(const TClientRequestMessage& message)
        {
            auto flags = InetToHost(message.Flags);
            auto cookie = InetToHost(message.Cookie);
            auto offset = InetToHost(message.Offset);
            auto length = InetToHost(message.Length);
            auto payload = ReadBuffer(length);

            if (offset + length > static_cast<ui64>(Device_->GetTotalSize())) {
                YT_LOG_WARNING("Received an out-of-range NBD_CMD_WRITE request (Offset: %v, Length: %v, Size: %v)",
                    offset,
                    length,
                    Device_->GetTotalSize());
                WriteServerResponse(EServerError::NBD_ENOSPC, cookie);
                return;
            }

            if (Device_->IsReadOnly()) {
                YT_LOG_WARNING("Received NBD_CMD_WRITE request for a read-only device");
                WriteServerResponse(EServerError::NBD_EPERM, cookie);
                return;
            }

            YT_LOG_DEBUG("Started serving NBD_CMD_WRITE request (Cookie: %x, Offset: %v, Length: %v, Flags: %v)",
                cookie,
                offset,
                length,
                flags);

            TWriteOptions options;
            if (Any(flags & ECommandFlags::NBD_CMD_FLAG_FUA)) {
                options.Flush = true;
            }

            Device_->Write(offset, payload, options)
                .Subscribe(
                    BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                        if (!error.IsOK()) {
                            YT_LOG_WARNING(error, "NBD_CMD_WRITE request failed (Cookie: %x)",
                                cookie);
                            WriteServerResponse(EServerError::NBD_EIO, cookie);
                            return;
                        }

                        YT_LOG_DEBUG("Finished serving NBD_CMD_READ request (Cookie: %x)",
                            cookie);

                        WriteServerResponse(EServerError::NBD_OK, cookie);
                    }));
        }

        void HandleClientFlushRequest(const TClientRequestMessage& message)
        {
            auto flags = InetToHost(message.Flags);
            auto cookie = InetToHost(message.Cookie);
            auto offset = InetToHost(message.Offset);
            auto length = InetToHost(message.Length);

            if (offset != 0) {
                YT_LOG_WARNING("Nonzero offset in NBD_CMD_FLUSH request");
                WriteServerResponse(EServerError::NBD_EINVAL, cookie);
                return;
            }

            if (length != 0) {
                YT_LOG_WARNING("Nonzero length in NBD_CMD_FLUSH request");
                WriteServerResponse(EServerError::NBD_EINVAL, cookie);
                return;
            }

            YT_LOG_DEBUG("Started serving NBD_CMD_FLUSH request (Cookie: %x, Flags: %v)",
                cookie,
                flags);

            Device_->Flush()
                .Subscribe(
                    BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                        if (!error.IsOK()) {
                            YT_LOG_WARNING(error, "NBD_CMD_FLUSH request failed (Cookie: %x)",
                                cookie);
                            WriteServerResponse(EServerError::NBD_EIO, cookie);
                            return;
                        }

                        YT_LOG_DEBUG("Finished serving NBD_CMD_FLUSH request (Cookie: %x)",
                            cookie);

                        WriteServerResponse(EServerError::NBD_OK, cookie);
                    }));
        }

        void HandleClientDisconnectRequest(const TClientRequestMessage& /*message*/)
        {
            YT_LOG_INFO("Received NBD_CMD_DISC request, closing connection");

            Abort_ = true;
        }

        void WriteServerResponse(EServerError error, ui64 cookie, TSharedRef payload = {})
        {
            ResponseInvoker_->Invoke(
                BIND([=, this, this_ = MakeStrong(this), payload = std::move(payload)] {
                    TServerResponseMessage message{
                        .Magic = HostToInet(TServerResponseMessage::ExpectedHostMagic),
                        .Error = HostToInet(error),
                        .Cookie = HostToInet(cookie),
                    };
                    WritePod(message);

                    if (payload) {
                        WriteBuffer(payload);
                    }
                }));
        }

        template <class T>
        void WritePod(const T& pod)
        {
            auto buffer = TSharedMutableRef::Allocate<TNbdNetworkBufferTag>(sizeof(T));
            std::copy(&pod, &pod + 1, reinterpret_cast<T*>(buffer.Begin()));
            WriteBuffer(buffer);
        }

        void WriteBuffer(const TSharedRef& buffer)
        {
            auto error = WaitFor(Connection_->Write(buffer));
            if (!error.IsOK()) {
                // WriteBuffer might be called asynchronously so abort connection gracefully (don't throw).
                YT_LOG_WARNING(error, "Failed to write buffer, aborting connection");
                Abort_ = true;
            }
        }

        TSharedRef ReadBuffer(size_t size)
        {
            auto buffer = TSharedMutableRef::Allocate<TNbdNetworkBufferTag>(size);
            for (size_t offset = 0; offset < size; ) {
                auto readSize = WaitFor(Connection_->Read(buffer.Slice(offset, size)))
                    .ValueOrThrow();

                offset += readSize;
            }
            return buffer;
        }

        template <class T>
        T ReadPod()
        {
            auto buffer = ReadBuffer(sizeof(T));
            T pod;
            std::copy(buffer.Begin(), buffer.End(), reinterpret_cast<char*>(&pod));
            return pod;
        }
    };

    void AcceptConnection()
    {
        Listener_->Accept().Subscribe(
            BIND(&TNbdServer::OnConnectionAccepted, MakeWeak(this))
                .Via(Invoker_));
    }

    void OnConnectionAccepted(const TErrorOr<IConnectionPtr>& connectionOrError)
    {
        if (!connectionOrError.IsOK()) {
            YT_LOG_INFO(connectionOrError, "Error accepting connection");
            return;
        }

        AcceptConnection();

        const auto& connection = connectionOrError.Value();
        auto handler = New<TConnectionHandler>(this, connection);
        handler->Run();
    }
};

DEFINE_REFCOUNTED_TYPE(TNbdServer)

////////////////////////////////////////////////////////////////////////////////

std::atomic<int> TNbdServer::NbdServerCount_;

////////////////////////////////////////////////////////////////////////////////

INbdServerPtr CreateNbdServer(
    TNbdServerConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    IPollerPtr poller,
    IInvokerPtr invoker)
{
    auto server = New<TNbdServer>(
        std::move(config),
        std::move(connection),
        std::move(poller),
        std::move(invoker));
    server->Start();
    return server;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
