#pragma once

#include "command.h"

#include <yt/ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadFileCommand
    : public TTypedCommand<NApi::TFileReaderOptions>
{
private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileReader;

public:
    TReadFileCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("offset", Options.Offset)
            .Optional();
        RegisterParameter("length", Options.Length)
            .Optional();
        RegisterParameter("file_reader", FileReader)
            .Default(nullptr);

    }

    void Execute(ICommandContextPtr context);

};

class TWriteFileCommand
    : public TTypedCommand<NApi::TFileWriterOptions>
{
private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr FileWriter;

public:
    TWriteFileCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("file_writer", FileWriter)
            .Default();
    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

