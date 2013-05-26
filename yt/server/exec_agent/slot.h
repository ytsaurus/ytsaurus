#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>

#include <ytlib/formats/format.h>

#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public TRefCounted
{
public:
    TSlot(const Stroka& path, int slotId, int userId);

    void Initialize();

    bool IsFree() const;
    int GetUserId() const;

    void Acquire();
    void InitSandbox();
    void Clean();
    void Release();

    IInvokerPtr GetInvoker();

    //! Makes symbolic link on target inside slot sandbox.
    /*!
     *  Throws if operation fails.
     */
    void MakeLink(
        const Stroka& linkName,
        const Stroka& targetPath,
        bool isExecutable);

    //! Writes data from produce to #fileName
    void MakeFile(
        const Stroka& fileName,
        NYTree::TYsonProducer producer,
        const NFormats::TFormat& format);

    const Stroka& GetWorkingDirectory() const;

private:
    volatile bool IsFree_;
    bool IsClean;

    Stroka Path;
    int SlotId;
    int UserId;

    Stroka SandboxPath;

    TActionQueuePtr SlotThread;

    NLog::TTaggedLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
