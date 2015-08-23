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

class TInsertRowsExecutor
    : public TRequestExecutor
{
public:
    TInsertRowsExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::SwitchArg UpdateArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn = true;
    TStringStream Stream;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

class TSelectRowsExecutor
    : public TRequestExecutor
{
public:
    TSelectRowsExecutor();

private:
    TCLAP::UnlabeledValueArg<Stroka> QueryArg;
    TCLAP::ValueArg<NTransactionClient::TTimestamp> TimestampArg;
    TCLAP::ValueArg<int> InputRowLimitArg;
    TCLAP::ValueArg<int> OutputRowLimitArg;
    TCLAP::ValueArg<int> RangeExpansionLimitArg;
    TCLAP::SwitchArg VerboseLoggingArg;
    TCLAP::SwitchArg EnableCodeCacheArg;
    TCLAP::ValueArg<int> MaxSubqueriesArg;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupRowsExecutor
    : public TRequestExecutor
{
public:
    TLookupRowsExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TCLAP::ValueArg<NTransactionClient::TTimestamp> TimestampArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn = true;
    TStringStream Stream;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

class TDeleteRowsExecutor
    : public TRequestExecutor
{
public:
    TDeleteRowsExecutor();

private:
    TCLAP::UnlabeledValueArg<NYPath::TRichYPath> PathArg;
    TUnlabeledStringArg ValueArg;

    bool UseStdIn = true;
    TStringStream Stream;

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
    virtual Stroka GetCommandName() const override;
    virtual TInputStream* GetInputStream() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
