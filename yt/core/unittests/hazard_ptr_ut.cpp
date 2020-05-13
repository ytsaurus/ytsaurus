#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/format.h>
#include <yt/core/misc/new.h>
#include <yt/core/misc/public.h>
#include <yt/core/misc/atomic_ptr.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestAllocator
{
public:
    explicit TTestAllocator(IOutputStream* output)
        : Output_(output)
    { }

    void* Allocate(size_t size)
    {
        *Output_ << 'A';
        ++AllocatedCount_;

        size += sizeof(void*);
        auto ptr = NYTAlloc::Allocate(size);
        auto* header = static_cast<TTestAllocator**>(ptr);
        *header = this;
        return header + 1;
    }

    static void Free(void* ptr)
    {
        auto* header = static_cast<TTestAllocator**>(ptr) - 1;
        auto* allocator = *header;

        *allocator->Output_ << 'F';
        ++allocator->DeallocatedCount_;

        NYTAlloc::Free(header);
    }

    ~TTestAllocator()
    {
        YT_VERIFY(AllocatedCount_ == DeallocatedCount_);
    }

private:
    IOutputStream* const Output_;
    size_t AllocatedCount_ = 0;
    size_t DeallocatedCount_ = 0;
};

class TSampleObject final
{
public:
    using TAllocator = TTestAllocator;
    using TEnableHazard = void;

    explicit TSampleObject(IOutputStream* output)
        : Output_(output)
    {
        *Output_ << 'C';
    }

    ~TSampleObject()
    {
        *Output_ << 'D';
    }

    void DoSomething()
    {
        *Output_ << '!';
    }

private:
    IOutputStream* const Output_;

};

TEST(THazardPtrTest, RefCountedPtrBehavior)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    {
        auto ptr = New<TSampleObject>(&allocator, &output);
        {
            auto anotherPtr = ptr;
            anotherPtr->DoSomething();
        }
        {
            auto anotherPtr = ptr;
            anotherPtr->DoSomething();
        }
        ptr->DoSomething();
    }

    EXPECT_STREQ("AC!!!D", output.Str().c_str());

    ScanDeleteList();

    EXPECT_STREQ("AC!!!DF", output.Str().c_str());
}

TEST(THazardPtrTest, DelayedDeallocation)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSampleObject>(&allocator, &output);
    ptr->DoSomething();

    auto hazardPtr = THazardPtr<TSampleObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!D", output.Str().c_str());

    ScanDeleteList();

    EXPECT_STREQ("AC!D", output.Str().c_str());

    hazardPtr.Reset();
    ScanDeleteList();

    EXPECT_STREQ("AC!DF", output.Str().c_str());
}

TEST(THazardPtrTest, CombinedLogic)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSampleObject>(&allocator, &output);
    ptr->DoSomething();

    auto ptrCopy = ptr;
    auto rawPtr = ptrCopy.Release();

    auto hazardPtr = THazardPtr<TSampleObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!", output.Str().c_str());

    ScheduleObjectDeletion(rawPtr, [] (void* ptr) {
        Unref(static_cast<TSampleObject*>(ptr));
    });

    ScanDeleteList();

    EXPECT_STREQ("AC!", output.Str().c_str());

    {
        hazardPtr.Reset();
        ScanDeleteList();

        EXPECT_STREQ("AC!D", output.Str().c_str());
    }

    {
        auto hazardPtr = THazardPtr<TSampleObject>::Acquire([&] {
            return rawPtr;
        });

        ScanDeleteList();
        EXPECT_STREQ("AC!D", output.Str().c_str());
    }

    {
        ScanDeleteList();
        EXPECT_STREQ("AC!DF", output.Str().c_str());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSamplePolymorphicObject
    : public TRefCounted
{
public:
    using TAllocator = TTestAllocator;
    using TEnableHazard = void;

    explicit TSamplePolymorphicObject(IOutputStream* output)
        : Output_(output)
    {
        *Output_ << 'C';
    }

    ~TSamplePolymorphicObject()
    {
        *Output_ << 'D';
    }

    void DoSomething()
    {
        *Output_ << '!';
    }

private:
    IOutputStream* const Output_;

};

TEST(THazardPtrTest, DelayedDeallocationPolymorphic)
{
    TStringStream output;
    TTestAllocator allocator(&output);

    auto ptr = New<TSamplePolymorphicObject>(&allocator, &output);
    ptr->DoSomething();

    auto hazardPtr = THazardPtr<TSamplePolymorphicObject>::Acquire([&] {
        return ptr.Get();
    });

    ptr = nullptr;

    EXPECT_STREQ("AC!D", output.Str().c_str());

    ScanDeleteList();

    EXPECT_STREQ("AC!D", output.Str().c_str());

    hazardPtr.Reset();
    ScanDeleteList();

    EXPECT_STREQ("AC!DF", output.Str().c_str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
