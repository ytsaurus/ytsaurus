#pragma once

#include <util/generic/stroka.h>

#include <vector>
#include <chrono>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroup
    : private TNonCopyable
{
public:
    TCGroup(const Stroka& parent, const Stroka& name);
    ~TCGroup();

    void AddMyself();

    void Create();
    void Destroy();

    std::vector<int> GetTasks();
    const Stroka& GetFullName() const;
private:
    Stroka FullName_;
    bool Created_;
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
