#pragma once

#include "executor.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TGetExecutor
    : public TTransactedExecutor
{
public:
    TGetExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

class TSetExecutor
    : public TTransactedExecutor
{
public:
    TSetExecutor();

private:
    TUnlabeledStringArg PathArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
    virtual TInputStream* GetInputStream() OVERRIDE;
};

//////////////////////////////////////////////////////////////////////////////////

class TRemoveExecutor
    : public TTransactedExecutor
{
public:
    TRemoveExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

class TListExecutor
    : public TTransactedExecutor
{
public:
    TListExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateExecutor
    : public TTransactedExecutor
{
public:
    TCreateExecutor();

private:
    typedef TCLAP::UnlabeledValueArg<NObjectServer::EObjectType> TTypeArg;
    TTypeArg TypeArg;

    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

class TLockExecutor
    : public TTransactedExecutor
{
public:
    TLockExecutor();

private:
    TUnlabeledStringArg PathArg;

    typedef TCLAP::ValueArg<NCypressClient::ELockMode> TModeArg;
    TModeArg ModeArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

class TCopyExecutor
    : public TTransactedExecutor
{
public:
    TCopyExecutor();

private:
    TUnlabeledStringArg SourcePathArg;
    TUnlabeledStringArg DestinationPathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

class TMoveExecutor
    : public TTransactedExecutor
{
public:
    TMoveExecutor();

private:
    TUnlabeledStringArg SourcePathArg;
    TUnlabeledStringArg DestinationPathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) OVERRIDE;
    virtual Stroka GetCommandName() const OVERRIDE;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
