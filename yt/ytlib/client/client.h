#pragma once

#include "../misc/ptr.h"

namespace NYT {

struct IProcessor;
class TRead;
class TWrite;

////////////////////////////////////////////////////////////////////////////////
// server

class TServer
{
public:

    TServer(const char* location);
    TServer(const char* location, const char* proxy); // redirection
    ~TServer();

    // internal use
    class TServerImpl;
    TServerImpl* GetImpl() const { return ~Impl; }

private:

    THolder<TServerImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
// transaction

class TTransaction
{
public:

    TTransaction(TServer& server);
    ~TTransaction();

    void Process(IProcessor& processor, TRead& input, TWrite& output);

    bool Commit();
    void Cancel();

    // internal use
    class TTransactionImpl;
    TTransactionImpl* GetImpl() const { return ~Impl; }

private:

    THolder<TTransactionImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

}
