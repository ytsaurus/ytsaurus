#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
struct TEnvironmentConfig
    : public TConfigurable
{
    Stroka Type;

    // Type-dependent configuration is stored as options.

    TEnvironmentConfig()
    {
        SetKeepOptions(true);
        Register("type", Type)
            .NonEmpty();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration for a collection of named environments.
class TEnvironmentManagerConfig
    : public TConfigurable
{
public:
    TEnvironmentManagerConfig()
    {
        Register("environments", Environments);
    }

    TEnvironmentConfigPtr FindEnvironment(const Stroka& name)
    {
        auto it = Environments.find(name);
        if (it == Environments.end()) {
            ythrow yexception() << Sprintf("No such environment %s", ~name);
        }
        return it->second;
    }

private:
    yhash_map<Stroka, TEnvironmentConfigPtr> Environments;

};

struct TJobManagerConfig
    : public TEnvironmentManagerConfig
{
    // TODO(babenko): read from cypress.
    Stroka SchedulerAddress;

    int  SlotCount;
    Stroka SlotLocation;

    TJobManagerConfig()
    {
        Register("scheduler_address", SchedulerAddress).NonEmpty();
        Register("slot_count", SlotCount)
            .Default(8);
        Register("slot_location", SlotLocation)
            .NonEmpty();
    }
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
