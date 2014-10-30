#pragma once

#include "executor.h"

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadTableExecutor
    : public TTransactedExecutor
{
public:
    TReadTableExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

//////////////////////////////////////////////////////////////////////////////////

class TWriteTableExecutor
    : public TTransactedExecutor
{
public:
    TWriteTableExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TUnlabeledStringArg ValueArg;
    TCLAP::ValueArg<Stroka> SortedByArg;

    bool UseStdIn;
    TStringStream Stream;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

class TTabletExecutor
    : public TRequestExecutor
{
public:
    TTabletExecutor();

protected:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::ValueArg<int> FirstTabletIndexArg;
    TCLAP::ValueArg<int> LastTabletIndexArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;

};

////////////////////////////////////////////////////////////////////////////////

class TMountTableExecutor
    : public TTabletExecutor
{
public:
    TMountTableExecutor();

private:
    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnmountTableExecutor
    : public TTabletExecutor
{
public:
    TUnmountTableExecutor();

private:
    TCLAP::SwitchArg ForceArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemountTableExecutor
    : public TTabletExecutor
{
public:
    TRemountTableExecutor();

private:
    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TReshardTableExecutor
    : public TTabletExecutor
{
public:
    TReshardTableExecutor();

private:
    TCLAP::UnlabeledMultiArg<Stroka> PivotKeysArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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
    TCLAP::ValueArg<NTransactionClient::TTimestamp> TimestampArg;
    TCLAP::ValueArg<int> InputRowLimitArg;
    TCLAP::ValueArg<int> OutputRowLimitArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
