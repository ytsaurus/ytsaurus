#pragma once

#include "public.h"
#include "format.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/error.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

//! Data type that can be read or written by a driver command.
DECLARE_ENUM(EDataType,
    (Null)
    (Binary)
    (Node)
    (Table)
);

////////////////////////////////////////////////////////////////////////////////

//! An instance of driver request.
struct TDriverRequest
{
    //! Command name to execute.
    Stroka CommandName;

    //! Stream used for reading command input.
    TSharedPtr<TInputStream> InputStream;

    //! Format used for reading the input.
    TFormat InputFormat;

    //! Stream where the command output is written.
    TSharedPtr<TOutputStream> OutputStream;

    //! Format used for writing the output.
    TFormat OutputFormat;

    //! A map containing command arguments.
    NYTree::IMapNodePtr Arguments;
};

////////////////////////////////////////////////////////////////////////////////

//! An instance of driver request.
struct TDriverResponse
{
    //! An error returned by the command, if any.
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

//! Command meta-descriptor.
/*!
 *  Contains various meta-information describing a given command type.
 */
struct TCommandDescriptor
{
    //! Name of the command.
    Stroka CommandName;

    //! Type of data expected by the command at #TDriverRequest::InputStream.
    EDataType InputType;

    //! Type of data written by the command to #TDriverRequest::OutputStream.
    EDataType OutputType;

    TCommandDescriptor(EDataType inputType, EDataType outputType)
        : InputType(inputType)
        , OutputType(outputType)
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! An instance of command execution engine.
/*! 
 *  Each driver instance maintains a collection of cached connections to 
 *  various YT subsystems (e.g. masters, scheduler).
 *  
 *  Requests are executed synchronously.
 *  
 *  IDriver instances are thread-safe and reentrant.
 */
struct IDriver
    : public TRefCounted
{
    //! Synchronously executes a given request. Returns the outcome.
    virtual TDriverResponse Execute(const TDriverRequest& request) = 0;

    //! Returns a descriptor for a command with a given name.
    virtual TCommandDescriptor GetDescriptor(const Stroka& commandName) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Creates an implementation of IDriver with a given configuration.
IDriverPtr CreateDriver(TDriverConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

