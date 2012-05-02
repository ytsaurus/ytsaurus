#pragma once

#include "common.h"
#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/error.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriverHost
{
    virtual ~IDriverHost()
    { }

    virtual TInputStream*  GetInputStream() = 0;
    virtual TOutputStream* GetOutputStream() = 0;
    virtual TOutputStream* GetErrorStream() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDriver
    : public TRefCounted
{
    virtual TError Execute(const Stroka& commandName, NYTree::INodePtr command) = 0;
    virtual TCommandDescriptor GetDescriptor(const Stroka& commandName) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TDriverConfigPtr config, IDriverHost* driverHost);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

