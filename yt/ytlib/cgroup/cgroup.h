#pragma once

#include <util/generic/stroka.h>

#include <vector>
#include <chrono>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroup
{
public:
    TCGroup(const Stroka& parent, const Stroka& name);

    void AddMyself();

    void Create();
    void Destroy();

    std::vector<int> GetTasks();
    const Stroka& GetFullName() const;
private:
    Stroka FullName_;
};

struct TCpuAcctStat
{
    std::chrono::nanoseconds user;
    std::chrono::nanoseconds system;
};

TCpuAcctStat GetCpuAccStat(const Stroka& fullName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
