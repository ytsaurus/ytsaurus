#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/formats/format.h>

#include <ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadJournalCommand
    : public TTypedCommand<NApi::TJournalReaderOptions>
{
private:
    NYPath::TRichYPath Path;

    virtual void OnLoaded() override
    {
        TCommandBase::OnLoaded();

        Path = Path.Normalize();
    }

public:
    TReadJournalCommand()
    {
        RegisterParameter("path", Path);
    }

    void Execute(ICommandContextPtr context);

};

class TWriteJournalCommand
    : public TTypedCommand<NApi::TJournalWriterOptions>
{
private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalWriter;

public:
    TWriteJournalCommand()
    {
        RegisterParameter("path", Path);
    }

    void Execute(ICommandContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
