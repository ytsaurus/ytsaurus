#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public TRefCounted
{
public:
    TSlot(const Stroka& path, const Stroka& name);

    bool IsEmpty() const;

    void Acquire();
    void Clean();

    IInvoker::TPtr GetInvoker();

    //! Makes symbolic link on target inside slot sandbox.
    void MakeLink(
        const Stroka& linkName, 
        const Stroka& targetPath,
        bool isExecutable);

    const Stroka& GetWorkingDirectory() const;

private:
    volatile bool IsEmpty_;

    Stroka Path;
    Stroka SandboxPath;

    TActionQueue::TPtr SlotThread;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
