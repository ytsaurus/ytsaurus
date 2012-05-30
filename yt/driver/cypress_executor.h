#pragma once

#include "executor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TGetExecutor
    : public TTransactedExecutor
{
public:
    TGetExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

//////////////////////////////////////////////////////////////////////////////////

class TRemoveExecutor
    : public TTransactedExecutor
{
public:
    TRemoveExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TListExecutor
    : public TTransactedExecutor
{
public:
    TListExecutor();

private:
    TUnlabeledStringArg PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
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

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLockExecutor
    : public TTransactedExecutor
{
public:
    TLockExecutor();

private:
    TUnlabeledStringArg PathArg;

    typedef TCLAP::ValueArg<NCypress::ELockMode> TModeArg;
    TModeArg ModeArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
    virtual Stroka GetDriverCommandName() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
