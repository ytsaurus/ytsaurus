#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler-side representation an exec-node.
class TExecNode
    : public TRefCounted
{
    //! Address as reported by master.
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    
    //! Jobs that are currently running on this node.
    DEFINE_BYVAL_RW_PROPERTY(std::vector<TJobPtr>, Jobs);

public:
    TExecNode(const Stroka& address);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
