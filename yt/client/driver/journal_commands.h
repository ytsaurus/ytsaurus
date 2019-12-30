#pragma once

#include "command.h"

#include <yt/client/formats/format.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReadJournalCommand
    : public TTypedCommand<NApi::TJournalReaderOptions>
{
public:
    TReadJournalCommand();

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalReader;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteJournalCommand
    : public TTypedCommand<NApi::TJournalWriterOptions>
{
public:
    TWriteJournalCommand();

private:
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalWriter;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTruncateJournalCommand
    : public TTypedCommand<NApi::TTruncateJournalOptions>
{
public:
    TTruncateJournalCommand();

private:
    NYPath::TYPath Path;
    i64 RowCount;

    virtual void DoExecute(ICommandContextPtr context) override;
};

} // namespace NYT::NDriver
