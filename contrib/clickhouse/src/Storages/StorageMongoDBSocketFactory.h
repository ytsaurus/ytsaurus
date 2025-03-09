#pragma once

#error #include <DBPoco/MongoDB/Connection.h>


namespace DB
{

class StorageMongoDBSocketFactory : public DBPoco::MongoDB::Connection::SocketFactory
{
public:
    DBPoco::Net::StreamSocket createSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout, bool secure) override;

private:
    static DBPoco::Net::StreamSocket createPlainSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout);
    static DBPoco::Net::StreamSocket createSecureSocket(const std::string & host, int port, DBPoco::Timespan connectTimeout);
};

}
