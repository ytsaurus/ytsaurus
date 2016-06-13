#pragma once

#include "command.h"

#include <yt/ytlib/formats/format.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/ypath/rich.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadJournalCommand
    : public TTypedCommand<NApi::TJournalReaderOptions>
{
private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalReader;

    virtual void OnLoaded() override
    {
        TCommandBase::OnLoaded();

        Path = Path.Normalize();
    }

public:
    TReadJournalCommand()
    {
        RegisterParameter("path", Path);
        RegisterParameter("journal_reader", JournalReader)
            .Default();
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
