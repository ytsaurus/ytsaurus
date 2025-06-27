#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "private.h"

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GenerateCounterId(TIdGenerator& generator, T invalidId, T maxId)
{
    T id;
    while (true) {
        id = T(generator.Next());
        // Beware of sentinels!
        if (id == invalidId) {
            // Just wait for the next attempt.
        } else if (id > maxId) {
            constexpr auto& Logger = NodeTrackerServerLogger;
            YT_LOG_ALERT("Counter id generator was reset");

            generator.Reset();
        } else {
            break;
        }
    }
    return id;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
