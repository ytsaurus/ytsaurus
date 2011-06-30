#include "client.h"
#include "io.h"
#include "processor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServer::TServerImpl
{
public:

    TServerImpl(const char* location)
        : Location(location)
    {}

    TServerImpl(const char* location, const char* proxy)
        : Location(location), Proxy(proxy)
    {}

private:

    Stroka Location;
    Stroka Proxy;
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(const char* location)
    : Impl(new TServerImpl(location))
{}

TServer::TServer(const char* location, const char* proxy)
    : Impl(new TServerImpl(location, proxy))
{}

TServer::~TServer()
{}

////////////////////////////////////////////////////////////////////////////////

class TTransaction::TTransactionImpl
{
public:

    TTransactionImpl(TServer::TServerImpl* server)
        : Server(server)
    {}

    void Process(IProcessor& processor, TRead& input, TWrite& output)
    {
        (void)processor;
        (void)input;
        (void)output;
    }

    bool Commit() { return false; }
    void Cancel() {}

private:

    TServer::TServerImpl* Server;
};

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(TServer& server)
    : Impl(new TTransactionImpl(server.GetImpl()))
{}

TTransaction::~TTransaction()
{}

void TTransaction::Process(IProcessor& processor, TRead& input, TWrite& output)
{
    Impl->Process(processor, input, output);
}

bool TTransaction::Commit()
{
    return Impl->Commit();
}

void TTransaction::Cancel()
{
    Impl->Cancel();
}

////////////////////////////////////////////////////////////////////////////////

}
