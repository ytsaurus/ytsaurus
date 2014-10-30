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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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
    TCLAP::SwitchArg NonRecursiveArg;
    TCLAP::SwitchArg ForceArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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
    TCLAP::SwitchArg RecursiveArg;
    TCLAP::SwitchArg IgnoreExistingArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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
    TCLAP::SwitchArg WaitableArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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
    TCLAP::SwitchArg PreserveAccountArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TLinkExecutor
    : public TTransactedExecutor
{
public:
    TLinkExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> TargetPathArg;
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> LinkPathArg;
    TCLAP::SwitchArg RecursiveArg;
    TCLAP::SwitchArg IgnoreExistingArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
