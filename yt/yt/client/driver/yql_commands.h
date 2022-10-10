#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartYqlQueryCommand
    : public TTypedCommand<NApi::TStartYqlQueryOptions>
{
public:
    TStartYqlQueryCommand();

private:
    TString Query;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
