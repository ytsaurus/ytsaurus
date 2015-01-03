#pragma once

#include "config.h"
#include "io_helpers.h"

#include <core/ytree/tree_builder.h>
#include <core/ytree/fluent.h>
#include <core/ytree/ypath_service.h>
#include <core/ytree/ypath_client.h>

#include <ytlib/misc/tclap_helpers.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/driver/public.h>

#include <ytlib/formats/format.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NDriver {

/////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExitCode,
    ((OK)(0))
    ((Error)(1))
);

class TExecutor
    : public TRefCounted
{
public:
    TExecutor();

    virtual void Execute(const std::vector<std::string>& args);
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

    virtual void DoExecute() = 0;
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

    TCLAP::ValueArg<Stroka> AuthenticatedUserArg;
    TCLAP::ValueArg<Stroka> FormatArg;
    TCLAP::ValueArg<Stroka> InputFormatArg;
    TCLAP::ValueArg<Stroka> OutputFormatArg;
    TCLAP::MultiArg<Stroka> OptArg;
    TCLAP::SwitchArg ResponseParametersArg;

    virtual void DoExecute() override;
    virtual void DoExecute(const TDriverRequest& request);

    NFormats::TFormat GetFormat(NFormats::EDataType dataType, const TNullable<NYTree::TYsonString>& yson);

    NYTree::IMapNodePtr GetParameters();

    // Construct args according to given options
    virtual void BuildParameters(NYson::IYsonConsumer* consumer);

    virtual TInputStream* GetInputStream();

    std::unique_ptr<TOutputStream> OutputStream_;
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

    virtual void BuildParameters(NYson::IYsonConsumer* consumer) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
