#pragma once

#include <Common/CurrentMetrics.h>
#include "clickhouse_config.h"
#include <Core/PostgreSQLProtocol.h>
#include <DBPoco/Net/TCPServerConnection.h>
#include "IServer.h"

#if USE_SSL
#   include <DBPoco/Net/SecureStreamSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric PostgreSQLConnection;
}

namespace DB
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

/** PostgreSQL wire protocol implementation.
 * For more info see https://www.postgresql.org/docs/current/protocol.html
 */
class PostgreSQLHandler : public DBPoco::Net::TCPServerConnection
{
public:
    PostgreSQLHandler(
        const DBPoco::Net::StreamSocket & socket_,
        IServer & server_,
        TCPServer & tcp_server_,
        bool ssl_enabled_,
        Int32 connection_id_,
        std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    void run() final;

private:
    LoggerPtr log = getLogger("PostgreSQLHandler");

    IServer & server;
    TCPServer & tcp_server;
    std::unique_ptr<Session> session;
    bool ssl_enabled = false;
    Int32 connection_id = 0;
    Int32 secret_key = 0;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
    std::shared_ptr<PostgreSQLProtocol::Messaging::MessageTransport> message_transport;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

#if USE_SSL
    std::shared_ptr<DBPoco::Net::SecureStreamSocket> ss;
#endif

    PostgreSQLProtocol::PGAuthentication::AuthenticationManager authentication_manager;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::PostgreSQLConnection};

    void changeIO(DBPoco::Net::StreamSocket & socket);

    bool startup();

    void establishSecureConnection(Int32 & payload_size, Int32 & info);

    void makeSecureConnectionSSL();

    void sendParameterStatusData(PostgreSQLProtocol::Messaging::StartupMessage & start_up_message);

    void cancelRequest();

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> receiveStartupMessage(int payload_size);

    void processQuery();

    static bool isEmptyQuery(const String & query);
};

}
