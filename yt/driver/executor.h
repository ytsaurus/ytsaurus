#pragma once

#include "config.h"

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/driver/public.h>
#include <ytlib/driver/config.h>
#include <ytlib/formats/format.h>

#include <ytlib/misc/tclap_helpers.h>
#include <tclap/CmdLine.h>

namespace NYT {
namespace NDriver {

/////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EExitCode,
    ((OK)(0))
    ((Error)(1))
);

////////////////////////////////////////////////////////////////////////////////

class TExecutor
    : public TRefCounted
{
public:
    TExecutor();

    virtual EExitCode Execute(const std::vector<std::string>& args);
    virtual Stroka GetCommandName() const = 0;

protected:
    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    TCLAP::CmdLine CmdLine;
    TCLAP::ValueArg<Stroka> ConfigArg;
    TCLAP::ValueArg<Stroka> FormatArg;
    TCLAP::ValueArg<Stroka> InputFormatArg;
    TCLAP::ValueArg<Stroka> OutputFormatArg;
    TCLAP::MultiArg<Stroka> ConfigOptArg;
    TCLAP::MultiArg<Stroka> OptArg;

    TExecutorConfigPtr Config;
    NDriver::IDriverPtr Driver;

    Stroka GetConfigFileName();
    void InitConfig();
    void ApplyConfigUpdates(NYTree::IYPathServicePtr service);
    
    NFormats::TFormat GetFormat(NFormats::EDataType dataType, const TNullable<NYTree::TYsonString>& yson);

    NYTree::IMapNodePtr GetArgs();
    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);

    virtual EExitCode DoExecute(const NDriver::TDriverRequest& request);
    virtual TInputStream* GetInputStream();
};

typedef TIntrusivePtr<TExecutor> TExecutorPtr;

////////////////////////////////////////////////////////////////////////////////

class TTransactedExecutor
    : public TExecutor
{
public:
    TTransactedExecutor(bool required = false);

protected:
    TCLAP::ValueArg<Stroka> TxArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
