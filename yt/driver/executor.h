#pragma once

#include "config.h"

#include <ytlib/misc/tclap_helpers.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/driver/public.h>
#include <ytlib/driver/config.h>

#include <ytlib/formats/format.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NDriver {

/////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EExitCode,
    ((OK)(0))
    ((Error)(1))
);

class TExecutor
    : public TRefCounted
{
public:
    TExecutor();

    virtual EExitCode Execute(const std::vector<std::string>& args);
    virtual Stroka GetCommandName() const = 0;

protected:
    TCLAP::CmdLine CmdLine;
    TCLAP::ValueArg<Stroka> ConfigArg;
    TCLAP::MultiArg<Stroka> ConfigOptArg;

    TExecutorConfigPtr Config;
    NDriver::IDriverPtr Driver;

    Stroka GetConfigFileName();
    void InitConfig();
    void ApplyConfigUpdates(NYTree::IYPathServicePtr service);

    virtual EExitCode DoExecute();
};

typedef TIntrusivePtr<TExecutor> TExecutorPtr;

////////////////////////////////////////////////////////////////////////////////

class TRequestExecutor
    : public TExecutor
{
public:
    TRequestExecutor();

protected:
    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    TCLAP::ValueArg<Stroka> FormatArg;
    TCLAP::ValueArg<Stroka> InputFormatArg;
    TCLAP::ValueArg<Stroka> OutputFormatArg;
    TCLAP::MultiArg<Stroka> OptArg;

    virtual EExitCode DoExecute();
    virtual EExitCode DoExecute(const TDriverRequest& request);

    NFormats::TFormat GetFormat(NFormats::EDataType dataType, const TNullable<NYTree::TYsonString>& yson);

    NYTree::IMapNodePtr GetArgs();

    // Construct args according to given options
    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);

    virtual TInputStream* GetInputStream();
};


////////////////////////////////////////////////////////////////////////////////

class TTransactedExecutor
    : public TRequestExecutor
{
public:
    explicit TTransactedExecutor(
        bool txRequired = false,
        bool txLabeled = true);

protected:
    TCLAP::ValueArg<NTransactionClient::TTransactionId> LabeledTxArg;
    TCLAP::UnlabeledValueArg<NTransactionClient::TTransactionId> UnlabeledTxArg;
    TCLAP::SwitchArg PingAncestorTxsArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
