#pragma once

#include "command.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/formats/format.h>

#include <ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TReadJournalRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;

    TReadJournalRequest()
    {
        RegisterParameter("path", Path);
    }

    virtual void OnLoaded() override
    {
        TTransactionalRequest::OnLoaded();

        Path = Path.Normalize();
    }
};

class TReadJournalCommand
    : public TTypedCommand<TReadJournalRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

struct TWriteJournalRequest
    : public TTransactionalRequest
{
    NYPath::TRichYPath Path;
    NYTree::INodePtr JournalWriter;

    TWriteJournalRequest()
    {
        RegisterParameter("path", Path);
    }
};

class TWriteJournalCommand
    : public TTypedCommand<TWriteJournalRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
