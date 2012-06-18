#pragma once

#include "config.h"

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/driver/public.h>
#include <ytlib/driver/config.h>
#include <ytlib/formats/format.h>

#include <ytlib/misc/tclap_helpers.h>
#include <tclap/CmdLine.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TExecutorBase
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TExecutorBase> TPtr;

    TExecutorBase();

    virtual void Execute(const std::vector<std::string>& args);

protected:
    typedef TCLAP::UnlabeledValueArg<Stroka> TUnlabeledStringArg;

    TCLAP::CmdLine CmdLine;
    TCLAP::ValueArg<Stroka> ConfigArg;
    TCLAP::ValueArg<Stroka> FormatArg;
    TCLAP::ValueArg<Stroka> InputFormatArg;
    TCLAP::ValueArg<Stroka> OutputFormatArg;
    TCLAP::MultiArg<Stroka> ConfigSetArg;
    TCLAP::MultiArg<Stroka> OptsArg;

    TExecutorConfigPtr Config;
    NDriver::IDriverPtr Driver;

    Stroka GetConfigFileName();
    void InitConfig();
    void ApplyConfigUpdates(NYTree::IYPathServicePtr service);
    
    NFormats::TFormat GetFormat(NFormats::EDataType dataType, const NYTree::TYson& custom);

    NYTree::IMapNodePtr GetArgs();
    void BuildOptions(NYTree::IYsonConsumer* consumer);
    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);

    virtual Stroka GetDriverCommandName() const = 0;

    virtual void DoExecute(const NDriver::TDriverRequest& request);
    virtual TInputStream* GetInputStream();
};

typedef TIntrusivePtr<TExecutorBase> TArgsBasePtr;

////////////////////////////////////////////////////////////////////////////////

class TTransactedExecutor
    : public TExecutorBase
{
public:
    TTransactedExecutor(bool required = false);

protected:
    TCLAP::ValueArg<Stroka> TxArg;

    virtual void BuildArgs(NYTree::IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
