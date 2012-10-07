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
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::MultiArg<Stroka> AttributeArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TSetExecutor
    : public TTransactedExecutor
{
public:
    TSetExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

//////////////////////////////////////////////////////////////////////////////////

class TRemoveExecutor
    : public TTransactedExecutor
{
public:
    TRemoveExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TListExecutor
    : public TTransactedExecutor
{
public:
    TListExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::MultiArg<Stroka> AttributeArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateExecutor
    : public TTransactedExecutor
{
public:
    TCreateExecutor();

private:
    typedef TCLAP::UnlabeledValueArg<NObjectClient::EObjectType> TTypeArg;
    TTypeArg TypeArg;

    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TLockExecutor
    : public TTransactedExecutor
{
public:
    TLockExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    typedef TCLAP::ValueArg<NCypressClient::ELockMode> TModeArg;
    TModeArg ModeArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TCopyExecutor
    : public TTransactedExecutor
{
public:
    TCopyExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> SourcePathArg;
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> DestinationPathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TMoveExecutor
    : public TTransactedExecutor
{
public:
    TMoveExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> SourcePathArg;
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> DestinationPathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TExistsExecutor
    : public TTransactedExecutor
{
public:
    TExistsExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
