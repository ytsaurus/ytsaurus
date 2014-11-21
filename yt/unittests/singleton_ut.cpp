#include "stdafx.h"
#include "framework.h"

#include <core/misc/at_exit_manager.h>
#include <core/misc/singleton.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TMyHeapObject
{
private:
    friend struct THeapInstanceMixin<TMyHeapObject>;

    TMyHeapObject()
    {
        ++CtorCalls;
    }

    ~TMyHeapObject()
    {
        ++DtorCalls;
    }

public:
    static TMyHeapObject* Get()
    {
        return TSingleton<TMyHeapObject>::Get();
    }

    static TMyHeapObject* TryGet()
    {
        return TSingleton<TMyHeapObject>::TryGet();
    }

    static void Reset()
    {
        CtorCalls = 0;
        DtorCalls = 0;
    }

    static int CtorCalls;
    static int DtorCalls;
};

int TMyHeapObject::CtorCalls = 0;
int TMyHeapObject::DtorCalls = 0;

struct TMyStaticObject
{
    TMyStaticObject()
    {
        ++CtorCalls;
        memset(&Payload, 0xAA, sizeof(Payload));
    }

    ~TMyStaticObject()
    {
        ++DtorCalls;
        memset(&Payload, 0xBB, sizeof(Payload));
    }

    static TMyStaticObject* Get()
    {
        return TSingleton<TMyStaticObject, TStaticInstanceMixin<TMyStaticObject>>::Get();
    }

    static TMyStaticObject* TryGet()
    {
        return TSingleton<TMyStaticObject, TStaticInstanceMixin<TMyStaticObject>>::TryGet();
    }

    static void Reset()
    {
        CtorCalls = 0;
        DtorCalls = 0;
    }

    static bool CheckObjectPayload(TMyStaticObject* object, int ch)
    {
        for (int i = 0; i < PayloadSize; ++i) {
            if ((int)((unsigned char)object->Payload[i]) != ch) {
                return false;
            }
        }
        return true;
    }

    static int CtorCalls;
    static int DtorCalls;

    static constexpr const int PayloadSize = 64;
    char Payload[PayloadSize];
};

int TMyStaticObject::CtorCalls = 0;
int TMyStaticObject::DtorCalls = 0;

class TSingletonTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        TMyHeapObject::Reset();
        TMyStaticObject::Reset();

        EXPECT_EQ(0, TMyHeapObject::CtorCalls);
        EXPECT_EQ(0, TMyHeapObject::DtorCalls);
    }

    TShadowingAtExitManager Manager_;
};

TEST_F(TSingletonTest, CtorAndDtorAreCalled)
{
    EXPECT_EQ(nullptr, TMyHeapObject::TryGet());
    auto instance1 = TMyHeapObject::Get();
    EXPECT_NE(nullptr, instance1);
    EXPECT_EQ(instance1, TMyHeapObject::Get());
    EXPECT_EQ(instance1, TMyHeapObject::TryGet());

    EXPECT_EQ(1, TMyHeapObject::CtorCalls);
    EXPECT_EQ(0, TMyHeapObject::DtorCalls);

    Manager_.FireAtExit();

    EXPECT_EQ(1, TMyHeapObject::CtorCalls);
    EXPECT_EQ(1, TMyHeapObject::DtorCalls);

    EXPECT_EQ(nullptr, TMyHeapObject::TryGet());
    auto instance2 = TMyHeapObject::Get();
    EXPECT_NE(nullptr, instance2);
    EXPECT_EQ(instance2, TMyHeapObject::Get());
    EXPECT_EQ(instance2, TMyHeapObject::TryGet());

    EXPECT_EQ(2, TMyHeapObject::CtorCalls);
    EXPECT_EQ(1, TMyHeapObject::DtorCalls);

    Manager_.FireAtExit();

    EXPECT_EQ(2, TMyHeapObject::CtorCalls);
    EXPECT_EQ(2, TMyHeapObject::DtorCalls);

    EXPECT_EQ(nullptr, TMyHeapObject::TryGet());
}

#ifndef _win_

TEST_F(TSingletonTest, ZeroAfterFork)
{
    // Object should be alive after first access.

    EXPECT_EQ(nullptr, TMyStaticObject::TryGet());
    auto instance = TMyStaticObject::Get();
    EXPECT_NE(nullptr, instance);
    EXPECT_EQ(instance, TMyStaticObject::Get());
    EXPECT_EQ(instance, TMyStaticObject::TryGet());

    EXPECT_TRUE(TMyStaticObject::CheckObjectPayload(instance, 0xAA));

    EXPECT_EQ(1, TMyStaticObject::CtorCalls);
    EXPECT_EQ(0, TMyStaticObject::DtorCalls);

    Manager_.FireAtExit();

    // Object should be dead or trashed after exit.
    EXPECT_EQ(nullptr, TMyStaticObject::TryGet());

    EXPECT_TRUE(
        TMyStaticObject::CheckObjectPayload(instance, 0xBA) ||
        TMyStaticObject::CheckObjectPayload(instance, 0xBB)
    );

    EXPECT_EQ(1, TMyStaticObject::CtorCalls);
    EXPECT_EQ(1, TMyStaticObject::DtorCalls);

    // Object should be resurrected in the same memory as it was seen before.

    EXPECT_EQ(instance, TMyStaticObject::Get());
    EXPECT_EQ(instance, TMyStaticObject::TryGet());

    EXPECT_TRUE(TMyStaticObject::CheckObjectPayload(instance, 0xAA));

    EXPECT_EQ(2, TMyStaticObject::CtorCalls);
    EXPECT_EQ(1, TMyStaticObject::DtorCalls);

    // Object should be zeroed after fork.

    int pid = fork();
    YCHECK(pid >= 0);

    if (pid > 0) {
        // Nothing happens in the parent.

        YCHECK(waitpid(pid, nullptr, 0) == pid);

        EXPECT_TRUE(TMyStaticObject::CheckObjectPayload(instance, 0xAA));

        EXPECT_EQ(instance, TMyStaticObject::Get());
        EXPECT_EQ(instance, TMyStaticObject::TryGet());

        EXPECT_EQ(2, TMyStaticObject::CtorCalls);
        EXPECT_EQ(1, TMyStaticObject::DtorCalls);
    } else {
        // Memory and instance pointer are zeroed in the child.

        EXPECT_TRUE(TMyStaticObject::CheckObjectPayload(instance, 0x00));
        EXPECT_EQ(nullptr, TMyStaticObject::TryGet());

        _exit(0);
    }

    Manager_.FireAtExit();

    EXPECT_EQ(2, TMyStaticObject::CtorCalls);
    EXPECT_EQ(2, TMyStaticObject::DtorCalls);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
