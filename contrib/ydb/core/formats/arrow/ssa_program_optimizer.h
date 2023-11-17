#pragma once

#include "program.h"

#include <contrib/ydb/core/protos/ssa.pb.h>
#include <contrib/ydb/core/tablet_flat/flat_dbase_scheme.h>

namespace NKikimr::NSsa {

void OptimizeProgram(TProgram& program);

}
