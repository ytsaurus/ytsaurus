#pragma once

#include "mutation_dumper.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJournalDumper
{
public:
    void Dump(const std::vector<NYTree::IMapNodePtr>& records);

    void AddMutationDumper(IMutationDumperPtr dumper);

private:
    std::vector<IMutationDumperPtr> MutationDumpers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
