#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TSetUserPasswordCommand
    : public TTypedCommand<NApi::TSetUserPasswordOptions>
{
public:
    TSetUserPasswordCommand();

private:
    TString User_;

    TString CurrentPasswordSha256_;
    TString NewPasswordSha256_;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
