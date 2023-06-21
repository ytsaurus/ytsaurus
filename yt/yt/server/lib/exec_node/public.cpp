#include "public.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

const TEnumIndexedVector<ESandboxKind, TString> SandboxDirectoryNames{
    "sandbox",
    "udf",
    "home",
    "pipes",
    "tmp",
    "cores",
    "logs"
};

const TString EmptyCpuSet("");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

