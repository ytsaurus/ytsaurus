#pragma once

#include <DBPoco/Net/StreamSocket.h>

#include <Common/callOnce.h>
#include <Common/SSHWrapper.h>
#include <Client/IServerConnection.h>
#include <Core/Defines.h>


#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <Interpreters/TablesStatus.h>
#include <Interpreters/Context_fwd.h>

#include <Compression/ICompressionCodec.h>

#include <Storages/MergeTree/RequestResponse.h>

#include <optional>

#include "clickhouse_config.h"

namespace DB
{

struct Settings;

class Connection;
struct ConnectionParameters;

using ConnectionPtr = std::shared_ptr<Connection>;
using Connections = std::vector<ConnectionPtr>;

class NativeReader;
class NativeWriter;


/** Connection with database server, to use by client.
  * How to use - see Core/Protocol.h
  * (Implementation of server end - see Server/TCPHandler.h)
  *
  * As 'default_database' empty string could be passed
  *  - in that case, server will use it's own default database.
  */
class Connection : public IServerConnection
{
    friend class MultiplexedConnections;

public:
    Connection(const String & host_, UInt16 port_,
        const String & default_database_,
        const String & user_, const String & password_,
        const SSHKey & ssh_private_key_,
        const String & jwt_,
        const String & quota_key_,
        const String & cluster_,
        const String & cluster_secret_,
        const String & client_name_,
        Protocol::Compression compression_,
        Protocol::Secure secure_);

    ~Connection() override;

    IServerConnection::Type getConnectionType() const override { return IServerConnection::Type::SERVER; }

    static ServerConnectionPtr createConnection(const ConnectionParameters & parameters, ContextPtr context);

    /// Set throttler of network traffic. One throttler could be used for multiple connections to limit total traffic.
    void setThrottler(const ThrottlerPtr & throttler_) override
    {
        throttler = throttler_;
    }

    /// Change default database. Changes will take effect on next reconnect.
    void setDefaultDatabase(const String & database) override;

    void getServerVersion(const ConnectionTimeouts & timeouts,
                          String & name,
                          UInt64 & version_major,
                          UInt64 & version_minor,
                          UInt64 & version_patch,
                          UInt64 & revision) override;

    UInt64 getServerRevision(const ConnectionTimeouts & timeouts) override;

    const String & getServerTimezone(const ConnectionTimeouts & timeouts) override;
    const String & getServerDisplayName(const ConnectionTimeouts & timeouts) override;

    /// For log and exception messages.
    const String & getDescription(bool with_extra = false) const override; /// NOLINT
    const String & getHost() const;
    UInt16 getPort() const;
    const String & getDefaultDatabase() const;

    Protocol::Compression getCompression() const { return compression; }

    std::vector<std::pair<String, String>> getPasswordComplexityRules() const override { return password_complexity_rules; }

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const NameToNameMap& query_parameters,
        const String & query_id_/* = "" */,
        UInt64 stage/* = QueryProcessingStage::Complete */,
        const Settings * settings/* = nullptr */,
        const ClientInfo * client_info/* = nullptr */,
        bool with_pending_data/* = false */,
        std::function<void(const Progress &)> process_progress_callback) override;

    void sendCancel() override;

    void sendData(const Block & block, const String & name/* = "" */, bool scalar/* = false */) override;

    void sendMergeTreeReadTaskResponse(const ParallelReadResponse & response) override;

    void sendExternalTablesData(ExternalTablesData & data) override;

    bool poll(size_t timeout_microseconds/* = 0 */) override;

    bool hasReadPendingData() const override;

    std::optional<UInt64> checkPacket(size_t timeout_microseconds/* = 0*/) override;

    Packet receivePacket() override;

    void forceConnected(const ConnectionTimeouts & timeouts) override;

    bool isConnected() const override { return connected; }

    bool checkConnected(const ConnectionTimeouts & timeouts) override { return connected && ping(timeouts); }

    void disconnect() override;


    /// Send prepared block of data (serialized and, if need, compressed), that will be read from 'input'.
    /// You could pass size of serialized/compressed block.
    void sendPreparedData(ReadBuffer & input, size_t size, const String & name = "");

    void sendReadTaskResponse(const String &);
    /// Send all scalars.
    void sendScalarsData(Scalars & data);
    /// Send parts' uuids to excluded them from query processing
    void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids);

    TablesStatusResponse getTablesStatus(const ConnectionTimeouts & timeouts,
                                         const TablesStatusRequest & request);

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

    DBPoco::Net::Socket * getSocket() { return socket.get(); }

    /// Each time read from socket blocks and async_callback is set, it will be called. You can poll socket inside it.
    void setAsyncCallback(AsyncCallback async_callback_)
    {
        async_callback = std::move(async_callback_);
        if (in)
            in->setAsyncCallback(async_callback);
        if (out)
            out->setAsyncCallback(async_callback);
    }

    bool haveMoreAddressesToConnect() const { return have_more_addresses_to_connect; }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;
#if USE_SSH
    SSHKey ssh_private_key;
#endif
    String quota_key;
    String jwt;

    /// For inter-server authorization
    String cluster;
    String cluster_secret;
    /// For DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET
    String salt;
    /// For DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2
    std::optional<UInt64> nonce;

    /// Address is resolved during the first connection (or the following reconnects)
    /// Use it only for logging purposes
    std::optional<DBPoco::Net::SocketAddress> current_resolved_address;

    /// For messages in log and in exceptions.
    String description;
    String full_description;
    void setDescription();

    /// Returns resolved address if it was resolved.
    std::optional<DBPoco::Net::SocketAddress> getResolvedAddress() const;

    String client_name;

    bool connected = false;

    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;
    UInt64 server_revision = 0;
    String server_timezone;
    String server_display_name;

    std::unique_ptr<DBPoco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;
    std::optional<UInt64> last_input_packet_type;

    String query_id;
    Protocol::Compression compression;        /// Enable data compression for communication.
    Protocol::Secure secure;             /// Enable data encryption for communication.

    /// What compression settings to use while sending data for INSERT queries and external tables.
    CompressionCodecPtr compression_codec;

    /** If not nullptr, used to limit network traffic.
      * Only traffic for transferring blocks is accounted. Other packets don't.
      */
    ThrottlerPtr throttler;

    std::vector<std::pair<String, String>> password_complexity_rules;

    /// From where to read query execution result.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    std::unique_ptr<NativeReader> block_in;
    std::unique_ptr<NativeReader> block_logs_in;
    std::unique_ptr<NativeReader> block_profile_events_in;

    /// Where to write data for INSERT.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    std::unique_ptr<NativeWriter> block_out;

    bool have_more_addresses_to_connect = false;

    /// Logger is created lazily, for avoid to run DNS request in constructor.
    class LoggerWrapper
    {
    public:
        explicit LoggerWrapper(Connection & parent_)
            : log(nullptr), parent(parent_)
        {
        }

        LoggerPtr get()
        {
            callOnce(log_initialized, [&] {
                log = getLogger("Connection (" + parent.getDescription() + ")");
            });

            return log;
        }

    private:
        OnceFlag log_initialized;
        LoggerPtr log;
        Connection & parent;
    };

    LoggerWrapper log_wrapper;

    AsyncCallback async_callback = {};

    void connect(const ConnectionTimeouts & timeouts);
    void sendHello();

#if USE_SSH
    void performHandshakeForSSHAuth();
#endif

    void sendAddendum();
    void receiveHello(const DBPoco::Timespan & handshake_timeout);

#if USE_SSL
    void sendClusterNameAndSalt();
#endif
    bool ping(const ConnectionTimeouts & timeouts);

    Block receiveData();
    Block receiveLogData();
    Block receiveDataImpl(NativeReader & reader);
    Block receiveProfileEvents();

    std::vector<String> receiveMultistringMessage(UInt64 msg_type) const;
    std::unique_ptr<Exception> receiveException() const;
    Progress receiveProgress() const;
    ParallelReadRequest receiveParallelReadRequest() const;
    InitialAllRangesAnnouncement receiveInitialParallelReadAnnouncement() const;
    ProfileInfo receiveProfileInfo() const;

    void initInputBuffers();
    void initBlockInput();
    void initBlockLogsInput();
    void initBlockProfileEventsInput();

    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const char * expected) const;
};

template <typename Conn>
class AsyncCallbackSetter
{
public:
    AsyncCallbackSetter(Conn * connection_, AsyncCallback async_callback) : connection(connection_)
    {
        connection->setAsyncCallback(std::move(async_callback));
    }

    ~AsyncCallbackSetter()
    {
        connection->setAsyncCallback({});
    }
private:
    Conn * connection;
};

}
