#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration of a single environment.
// TODO(babenko): consider renaming to TEnvironmentConfig
struct TEnvironment
    : public TConfigurable
{
    Stroka Type;
    NYTree::INodePtr Config;

    TEnvironment()
    {
        Register("type", Type)
            .NonEmpty();
        // TODO(babenko): consider placing everything into options
        Register("config", Config);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes configuration for a collection of names environments.
// TODO(babenko): consider renaming to TEnvironmentMapConfig
class TEnvironmentMap
    : public TConfigurable
{
public:
    TEnvironmentMap();

    TEnvironmentPtr FindEnvironment(const Stroka& name);

private:
    yhash_map<Stroka, TEnvironmentPtr> Environments;

};

struct TJobManagerConfig
    : public TEnvironmentMap
{
    typedef TIntrusivePtr<TConfig> TPtr;

    // ToDo: read from cypress.
    Stroka SchedulerAddress;

    int  SlotCount;
    Stroka SlotLocation;

    TConfig()
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
