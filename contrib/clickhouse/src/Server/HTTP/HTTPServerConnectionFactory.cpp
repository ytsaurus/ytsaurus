#include <Server/HTTP/HTTPServerConnectionFactory.h>

#include <Server/HTTP/HTTPServerConnection.h>

namespace DB
{
HTTPServerConnectionFactory::HTTPServerConnectionFactory(
    HTTPContextPtr context_, DBPoco::Net::HTTPServerParams::Ptr params_, HTTPRequestHandlerFactoryPtr factory_, const ProfileEvents::Event & read_event_, const ProfileEvents::Event & write_event_)
    : context(std::move(context_)), params(params_), factory(factory_), read_event(read_event_), write_event(write_event_)
{
    DB_poco_check_ptr(factory);
}

DBPoco::Net::TCPServerConnection * HTTPServerConnectionFactory::createConnection(const DBPoco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    return new HTTPServerConnection(context, tcp_server, socket, params, factory, read_event, write_event);
}

DBPoco::Net::TCPServerConnection * HTTPServerConnectionFactory::createConnection(const DBPoco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data)
{
    return new HTTPServerConnection(context, tcp_server, socket, params, factory, stack_data.forwarded_for, read_event, write_event);
}

}
