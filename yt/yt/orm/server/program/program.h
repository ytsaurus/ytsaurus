#pragma once

#include <yt/yt/orm/server/master/db_version_getter.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>

namespace NYT::NOrm::NServer::NProgram {

////////////////////////////////////////////////////////////////////////////////

template <class TDerivedProgram, class TMasterConfig>
class TMasterProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<TMasterConfig>
    , public NMaster::TDBVersionGetter
{
public:
    explicit TMasterProgram(int dbVersion);

    virtual void PrintVersionAndExit() override;

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override;

private:
    const int DBVersion_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NProgram

#define PROGRAM_INL_H_
#include "program-inl.h"
#undef PROGRAM_INL_H_
