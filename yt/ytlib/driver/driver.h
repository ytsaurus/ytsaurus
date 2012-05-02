#pragma once

#include "common.h"
#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/error.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>
// TODO: consider using forward declarations.
#include <ytlib/election/leader_lookup.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>
#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>

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
};

////////////////////////////////////////////////////////////////////////////////

IDriverPtr CreateDriver(TDriverConfigPtr config, IDriverHost* driverHost);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

