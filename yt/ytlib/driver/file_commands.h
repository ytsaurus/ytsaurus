#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadFileRequest
    : public TTransactionalRequest
    , public TSuppressableAccessTrackingRequest
{
    NYPath::TRichYPath Path;
    TNullable<i64> Offset;
    TNullable<i64> Length;
    NYTree::INodePtr FileReader;

    TReadFileRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("offset", Offset)
            .Default(Null);
        RegisterParameter("length", Length)
            .Default(Null);
        RegisterParameter("file_reader", FileReader)
            .Default(nullptr);
    }
};

class TReadFileCommand
    : public TTypedCommand<TReadFileRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TWriteFileRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileWriter;

    TWriteFileRequest()
    {
        RegisterParameter("path", Path);
        RegisterParameter("file_writer", FileWriter)
            .Default(nullptr);
    }
};

class TWriteFileCommand
    : public TTypedCommand<TWriteFileRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

