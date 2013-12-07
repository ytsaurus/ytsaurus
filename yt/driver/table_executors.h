#pragma once

#include "executor.h"

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadExecutor
    : public TTransactedExecutor
{
public:
    TReadExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TWriteExecutor
    : public TTransactedExecutor
{
public:
    TWriteExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TUnlabeledStringArg ValueArg;
    TCLAP::ValueArg<Stroka> SortedByArg;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

class TMountExecutor
    : public TRequestExecutor
{
public:
    TMountExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::ValueArg<int> FirstTabletIndexArg;
    TCLAP::ValueArg<int> LastTabletIndexArg;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnmountExecutor
    : public TRequestExecutor
{
public:
    TUnmountExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::ValueArg<int> FirstTabletIndexArg;
    TCLAP::ValueArg<int> LastTabletIndexArg;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TInsertExecutor
    : public TRequestExecutor
{
public:
    TInsertExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::SwitchArg UpdateArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

class TSelectExecutor
    : public TRequestExecutor
{
public:
    TSelectExecutor();

private:
    TCLAP::UnlabeledValueArg<Stroka> QueryArg;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupExecutor
    : public TRequestExecutor
{
public:
    TLookupExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::UnlabeledValueArg<Stroka> KeyArg;
    TCLAP::ValueArg<NTransactionClient::TTimestamp> TimestampArg;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TDeleteExecutor
    : public TRequestExecutor
{
public:
    TDeleteExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::UnlabeledValueArg<Stroka> KeyArg;

    virtual void BuildArgs(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
