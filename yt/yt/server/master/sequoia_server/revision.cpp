#include "revision.h"

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(std::optional<TSequoiaRevision>, CurrentSequoiaRevision);

std::optional<TSequoiaRevision> GetCurrentSequoiaRevision()
{
    return CurrentSequoiaRevision();
}

TSequoiaRevisionGuard::TSequoiaRevisionGuard(TSequoiaRevision revision)
{
    auto& sequoiaRevision = CurrentSequoiaRevision();
    YT_VERIFY(!sequoiaRevision);
    sequoiaRevision = revision;
}

TSequoiaRevisionGuard::~TSequoiaRevisionGuard()
{
    auto& sequoiaRevision = CurrentSequoiaRevision();
    YT_VERIFY(sequoiaRevision);
    sequoiaRevision.reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
