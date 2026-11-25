#include <yt/yt/flow/library/cpp/misc/cow_tree_detail/cow_tree_value.h>

#include <yt/yt/core/test_framework/framework.h>

#include <vector>

using namespace NYT::NFlow;
using namespace NYT::NFlow::NCowTreeInternals;

////////////////////////////////////////////////////////////////////////////////

//! Enumerates special member functions.
DEFINE_ENUM(EActionType,
    (Constructor)
    (Destructor)
    (CopyConstructor)
    (MoveConstructor)
    (CopyAssignment)
    (MoveAssignment));

//! Helper class to track construction/destruction.
struct TTrackedObject
{
    static std::vector<EActionType> Actions;

    int Value;

    TTrackedObject(int value = 0)
        : Value(value)
    {
        Actions.push_back(EActionType::Constructor);
    }

    ~TTrackedObject()
    {
        Actions.push_back(EActionType::Destructor);
    }

    TTrackedObject(const TTrackedObject& other)
        : Value(other.Value)
    {
        Actions.push_back(EActionType::CopyConstructor);
    }

    TTrackedObject(TTrackedObject&& other) noexcept
        : Value(other.Value)
    {
        Actions.push_back(EActionType::MoveConstructor);
        other.Value = -1;
    }

    TTrackedObject& operator=(const TTrackedObject& other)
    {
        Actions.push_back(EActionType::CopyAssignment);
        if (this != &other) {
            Value = other.Value;
        }
        return *this;
    }

    TTrackedObject& operator=(TTrackedObject&& other) noexcept
    {
        Actions.push_back(EActionType::MoveAssignment);
        if (this != &other) {
            Value = other.Value;
            other.Value = -1;
        }
        return *this;
    }

    static void ResetActions()
    {
        Actions.clear();
    }

    static void CheckActions(const std::vector<EActionType>& expectedActions)
    {
        EXPECT_EQ(Actions, expectedActions);
    }
};

std::vector<EActionType> TTrackedObject::Actions;

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolderDeathTest, ReportsNonStandardConstructorException)
{
    struct TConstructorError
    { };

    struct TValue
    {
        TValue()
        {
            throw TConstructorError{};
        }
    };

    TValueHolder<TValue> holder;
    EXPECT_DEATH(holder.Set(), "Failed to construct a CoW tree slot.*TConstructorError");
}

TEST(TValueHolder, DefaultConstruction)
{
    TValueHolder<int> holder;
    // Should not crash - holder is in empty state
}

TEST(TValueHolder, SetWithLValueReference)
{
    TValueHolder<int> holder;
    int value = 42;
    holder.Set(value);
    value = 24;

    EXPECT_EQ(holder.Get(), 42);
    holder.Clear();
}

TEST(TValueHolder, SetWithRValueReference)
{
    TValueHolder<int> holder;
    holder.Set(100);

    EXPECT_EQ(holder.Get(), 100);
    holder.Clear();
}

TEST(TValueHolder, SetWithConstLValueReference)
{
    TValueHolder<int> holder;
    const int value = 42;
    holder.Set(value);

    EXPECT_EQ(holder.Get(), 42);
    holder.Clear();
}

TEST(TValueHolder, GetLValueReference)
{
    TValueHolder<int> holder;
    holder.Set(42);

    int& ref = holder.Get();
    EXPECT_EQ(ref, 42);

    ref = 100;
    EXPECT_EQ(holder.Get(), 100);
    holder.Clear();
}

TEST(TValueHolder, GetConstLValueReference)
{
    TValueHolder<int> holder;
    holder.Set(42);

    const TValueHolder<int>& constHolder = holder;
    const int& ref = constHolder.Get();
    EXPECT_EQ(ref, 42);
    holder.Clear();
}

TEST(TValueHolder, GetRValueReference)
{
    TValueHolder<int> holder;
    holder.Set(42);

    int&& rref = std::move(holder).Get();
    EXPECT_EQ(rref, 42);
    // Moving out of a holder intentionally leaves it up to Clear() — the contract under test.
    holder.Clear(); // NOLINT(bugprone-use-after-move)
}

TEST(TValueHolder, GetConstRValueReference)
{
    TValueHolder<int> holder;
    holder.Set(42);

    const TValueHolder<int>& constHolder = holder;
    const int&& rref = std::move(constHolder).Get();
    EXPECT_EQ(rref, 42);
    holder.Clear();
}

TEST(TValueHolder, CopyFromAnotherHolder)
{
    TValueHolder<int> holder1;
    holder1.Set(42);

    TValueHolder<int> holder2;
    holder2.CopyFrom(holder1);

    EXPECT_EQ(holder2.Get(), 42);
    EXPECT_EQ(holder1.Get(), 42);
    holder1.Clear();
    holder2.Clear();
}

TEST(TValueHolder, MoveFromAnotherHolder)
{
    TValueHolder<int> holder1;
    holder1.Set(42);

    TValueHolder<int> holder2;
    holder2.MoveFrom(std::move(holder1));

    EXPECT_EQ(holder2.Get(), 42);
    holder2.Clear();
}

TEST(TValueHolder, WithString)
{
    TValueHolder<std::string> holder;
    holder.Set(std::string("Hello, World!"));

    EXPECT_EQ(holder.Get(), "Hello, World!");
    holder.Clear();
}

TEST(TValueHolder, WithStringLValue)
{
    TValueHolder<std::string> holder;
    std::string str = "Test String";
    holder.Set(str);

    EXPECT_EQ(holder.Get(), "Test String");
    holder.Get() = "Test Changed String";
    EXPECT_EQ(holder.Get(), "Test Changed String");
    EXPECT_EQ(str, "Test String");
    holder.Clear();
}

TEST(TValueHolder, WithStringRValue)
{
    TValueHolder<std::string> holder;
    std::string str = "Test String";
    holder.Set(std::move(str));

    EXPECT_EQ(holder.Get(), "Test String");
    // str is now in moved-from state
    holder.Clear();
}

TEST(TValueHolder, TrackedObjectCopyConstruction)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> holder;
    TTrackedObject obj(42);

    holder.Set(obj);

    EXPECT_EQ(holder.Get().Value, 42);
    EXPECT_EQ(obj.Value, 42);

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::CopyConstructor,
    });

    holder.Clear();

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::CopyConstructor,
        EActionType::Destructor,
    });
}

TEST(TValueHolder, TrackedObjectMoveConstruction)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> holder;
    TTrackedObject obj(42);

    holder.Set(std::move(obj));

    EXPECT_EQ(holder.Get().Value, 42);

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
    });

    holder.Clear();

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
    });
}

TEST(TValueHolder, TrackedObjectCopyFromHolder)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> holder1;
    holder1.Set(TTrackedObject(42));

    TValueHolder<TTrackedObject> holder2;
    holder2.CopyFrom(holder1);

    EXPECT_EQ(holder2.Get().Value, 42);
    EXPECT_EQ(holder1.Get().Value, 42);

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::CopyConstructor,
    });

    holder1.Clear();
    holder2.Clear();

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::CopyConstructor,
        EActionType::Destructor,
        EActionType::Destructor,
    });
}

TEST(TValueHolder, TrackedObjectMoveFromHolder)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> holder1;
    holder1.Set(TTrackedObject(42));

    TValueHolder<TTrackedObject> holder2;
    holder2.MoveFrom(std::move(holder1));

    EXPECT_EQ(holder2.Get().Value, 42);

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
    });

    holder2.Clear();

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::Destructor,
    });
}

TEST(TValueHolder, MultipleSetOperations)
{
    TValueHolder<int> holder;

    holder.Set(10);
    EXPECT_EQ(holder.Get(), 10);

    holder.Clear();
    holder.Set(20);
    EXPECT_EQ(holder.Get(), 20);

    holder.Clear();
    holder.Set(30);
    EXPECT_EQ(holder.Get(), 30);
    holder.Clear();
}

TEST(TValueHolder, WithUniquePtr)
{
    TValueHolder<std::unique_ptr<int>> holder;
    holder.Set(std::make_unique<int>(42));

    EXPECT_EQ(*holder.Get(), 42);
    holder.Clear();
}

TEST(TValueHolder, WithVector)
{
    TValueHolder<std::vector<int>> holder;
    std::vector<int> vec = {1, 2, 3, 4, 5};
    holder.Set(vec);

    EXPECT_EQ(holder.Get().size(), 5u);
    EXPECT_EQ(holder.Get()[0], 1);
    EXPECT_EQ(holder.Get()[4], 5);
    holder.Clear();
}

TEST(TValueHolder, ModifyThroughReference)
{
    TValueHolder<std::vector<int>> holder;
    holder.Set(std::vector<int>{1, 2, 3});

    auto& vec = holder.Get();
    vec.push_back(4);
    vec.push_back(5);

    EXPECT_EQ(holder.Get(), (std::vector<int>{1, 2, 3, 4, 5}));
    holder.Clear();
}

TEST(TValueHolder, ConstCorrectness)
{
    TValueHolder<int> holder;
    holder.Set(42);

    const TValueHolder<int>& constRef = holder;
    const int& value = constRef.Get();
    EXPECT_EQ(value, 42);

    holder.Clear();
}

TEST(TValueHolder, RValueGetDoesNotInvalidate)
{
    TValueHolder<std::string> holder;
    holder.Set(std::string("test"));

    std::string moved = std::move(holder).Get();
    EXPECT_EQ(moved, "test");
    // Moving out of a holder intentionally leaves it up to Clear() — the contract under test.
    holder.Clear(); // NOLINT(bugprone-use-after-move)
}

TEST(TValueHolder, ChainedOperations)
{
    TValueHolder<int> holder1;
    holder1.Set(10);

    TValueHolder<int> holder2;
    holder2.CopyFrom(holder1);

    TValueHolder<int> holder3;
    holder3.CopyFrom(holder2);

    EXPECT_EQ(holder1.Get(), 10);
    EXPECT_EQ(holder2.Get(), 10);
    EXPECT_EQ(holder3.Get(), 10);
    holder1.Clear();
    holder2.Clear();
    holder3.Clear();
}

TEST(TValueHolder, SetOverwritesPreviousValue)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> holder;
    holder.Set(TTrackedObject(10));
    holder.Clear();
    holder.Set(TTrackedObject(20));

    EXPECT_EQ(holder.Get().Value, 20);

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::Destructor,
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
    });

    holder.Clear();

    TTrackedObject::CheckActions({
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::Destructor,
        EActionType::Constructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::Destructor,
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolder, HolderOrValueAssignRegularTypeWithString)
{
    std::string dst = "old";
    std::string src = "new";

    HolderOrValueAssign(dst, src);

    EXPECT_EQ(dst, "new");
    EXPECT_EQ(src, "new");
}

TEST(TValueHolder, HolderOrValueAssignRegularTypeWithStringMove)
{
    std::string dst = "old";
    std::string src = "new";

    HolderOrValueAssign(dst, std::move(src));

    EXPECT_EQ(dst, "new");
}

TEST(TValueHolder, HolderOrValueAssignValueHolderWithRValue)
{
    TValueHolder<int> dst;
    TValueHolder<int> src;
    src.Set(42);

    HolderOrValueAssign(dst, std::move(src));

    EXPECT_EQ(dst.Get(), 42);
    dst.Clear();
}

TEST(TValueHolder, HolderOrValueAssignValueHolderWithLValue)
{
    TValueHolder<int> dst;
    TValueHolder<int> src;
    src.Set(42);

    HolderOrValueAssign(dst, src);

    EXPECT_EQ(dst.Get(), 42);
    EXPECT_EQ(src.Get(), 42);
    dst.Clear();
    src.Clear();
}

TEST(TValueHolder, HolderOrValueAssignValueHolderWithTrackedObject)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> dst;
    TValueHolder<TTrackedObject> src;
    src.Set(TTrackedObject(42));

    TTrackedObject::ResetActions();
    HolderOrValueAssign(dst, src);

    EXPECT_EQ(dst.Get().Value, 42);
    EXPECT_EQ(src.Get().Value, 42);

    TTrackedObject::CheckActions({
        EActionType::CopyConstructor,
    });

    dst.Clear();
    src.Clear();
}

TEST(TValueHolder, HolderOrValueAssignValueHolderWithTrackedObjectMove)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> dst;
    TValueHolder<TTrackedObject> src;
    src.Set(TTrackedObject(42));

    TTrackedObject::ResetActions();
    HolderOrValueAssign(dst, std::move(src));

    EXPECT_EQ(dst.Get().Value, 42);

    TTrackedObject::CheckActions({
        EActionType::MoveConstructor,
        EActionType::Destructor,
    });

    dst.Clear();
}

TEST(TValueHolder, HolderOrValueAssignValueHolderWithRegular)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> dst;
    TTrackedObject src(42);

    TTrackedObject::ResetActions();
    HolderOrValueAssign(dst, src);

    EXPECT_EQ(dst.Get().Value, 42);
    EXPECT_EQ(src.Value, 42);

    TTrackedObject::CheckActions({
        EActionType::CopyConstructor,
    });

    dst.Clear();
}

TEST(TValueHolder, HolderOrValueAssignValueHolderWithRegularMove)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> dst;
    TTrackedObject src(42);

    TTrackedObject::ResetActions();
    HolderOrValueAssign(dst, std::move(src));

    EXPECT_EQ(dst.Get().Value, 42);

    TTrackedObject::CheckActions({
        EActionType::MoveConstructor,
    });

    dst.Clear();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolder, HolderOrValueClearRegularTypeString)
{
    std::string value = "test";

    // Should do nothing for regular types
    HolderOrValueClear(value);

    EXPECT_EQ(value, "test");
}

TEST(TValueHolder, HolderOrValueClearValueHolder)
{
    TValueHolder<int> holder;
    holder.Set(42);

    // Should clear the holder
    HolderOrValueClear(holder);

    // Holder is now empty, can set again
    holder.Set(100);
    EXPECT_EQ(holder.Get(), 100);
    holder.Clear();
}

TEST(TValueHolder, HolderOrValueClearValueHolderWithTrackedObject)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> holder;
    holder.Set(TTrackedObject(42));

    TTrackedObject::ResetActions();
    HolderOrValueClear(holder);

    TTrackedObject::CheckActions({
        EActionType::Destructor,
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolder, HolderOrValueMoveEmptyRange)
{
    TValueHolder<int> src[3];
    TValueHolder<int> dst[3];

    // Move zero elements
    HolderOrValueMove(src, src, dst);
}

TEST(TValueHolder, HolderOrValueMoveSingleElement)
{
    TValueHolder<int> src[1];
    src[0].Set(42);

    TValueHolder<int> dst[1];

    HolderOrValueMove(src, src + 1, dst);

    EXPECT_EQ(dst[0].Get(), 42);
    dst[0].Clear();
}

TEST(TValueHolder, HolderOrValueMoveMultipleElements)
{
    TValueHolder<int> src[3];
    src[0].Set(10);
    src[1].Set(20);
    src[2].Set(30);

    TValueHolder<int> dst[3];

    HolderOrValueMove(src, src + 3, dst);

    EXPECT_EQ(dst[0].Get(), 10);
    EXPECT_EQ(dst[1].Get(), 20);
    EXPECT_EQ(dst[2].Get(), 30);

    dst[0].Clear();
    dst[1].Clear();
    dst[2].Clear();
}

TEST(TValueHolder, HolderOrValueMovePartialRange)
{
    TValueHolder<int> src[5];
    src[0].Set(10);
    src[1].Set(20);
    src[2].Set(30);
    src[3].Set(40);
    src[4].Set(50);

    TValueHolder<int> dst[3];

    // Move only elements [1, 4) -> [0, 3)
    HolderOrValueMove(src + 1, src + 4, dst);

    EXPECT_EQ(dst[0].Get(), 20);
    EXPECT_EQ(dst[1].Get(), 30);
    EXPECT_EQ(dst[2].Get(), 40);

    EXPECT_EQ(src[0].Get(), 10);
    EXPECT_EQ(src[4].Get(), 50);

    src[0].Clear();
    src[4].Clear();
    dst[0].Clear();
    dst[1].Clear();
    dst[2].Clear();
}

TEST(TValueHolder, HolderOrValueMoveWithTrackedObject)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> src[2];
    src[0].Set(TTrackedObject(10));
    src[1].Set(TTrackedObject(20));

    TValueHolder<TTrackedObject> dst[2];

    TTrackedObject::ResetActions();
    HolderOrValueMove(src, src + 2, dst);

    EXPECT_EQ(dst[0].Get().Value, 10);
    EXPECT_EQ(dst[1].Get().Value, 20);

    TTrackedObject::CheckActions({
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
    });

    dst[0].Clear();
    dst[1].Clear();
}

TEST(TValueHolder, HolderOrValueMoveOverlappingRanges)
{
    // Test moving within the same array (backward direction)
    TValueHolder<int> arr[5];
    arr[2].Set(10);
    arr[3].Set(20);
    arr[4].Set(30);

    // Move [2, 5) to [0, 3)
    HolderOrValueMove(arr + 2, arr + 5, arr);

    EXPECT_EQ(arr[0].Get(), 10);
    EXPECT_EQ(arr[1].Get(), 20);
    EXPECT_EQ(arr[2].Get(), 30);

    arr[0].Clear();
    arr[1].Clear();
    arr[2].Clear();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolder, HolderOrValueMoveBackwardEmptyRange)
{
    TValueHolder<int> src[3];
    TValueHolder<int> dst[3];

    // Move zero elements
    HolderOrValueMoveBackward(src, src, dst);
}

TEST(TValueHolder, HolderOrValueMoveBackwardSingleElement)
{
    TValueHolder<int> src[1];
    src[0].Set(42);

    TValueHolder<int> dst[1];

    HolderOrValueMoveBackward(src, src + 1, dst + 1);

    EXPECT_EQ(dst[0].Get(), 42);
    dst[0].Clear();
}

TEST(TValueHolder, HolderOrValueMoveBackwardMultipleElements)
{
    TValueHolder<int> src[3];
    src[0].Set(10);
    src[1].Set(20);
    src[2].Set(30);

    TValueHolder<int> dst[3];

    HolderOrValueMoveBackward(src, src + 3, dst + 3);

    EXPECT_EQ(dst[0].Get(), 10);
    EXPECT_EQ(dst[1].Get(), 20);
    EXPECT_EQ(dst[2].Get(), 30);

    dst[0].Clear();
    dst[1].Clear();
    dst[2].Clear();
}

TEST(TValueHolder, HolderOrValueMoveBackwardPartialRange)
{
    TValueHolder<int> src[5];
    src[0].Set(10);
    src[1].Set(20);
    src[2].Set(30);
    src[3].Set(40);
    src[4].Set(50);

    TValueHolder<int> dst[5];

    // Move elements [1, 4) backward to positions [2, 5)
    HolderOrValueMoveBackward(src + 1, src + 4, dst + 5);

    EXPECT_EQ(dst[2].Get(), 20);
    EXPECT_EQ(dst[3].Get(), 30);
    EXPECT_EQ(dst[4].Get(), 40);

    EXPECT_EQ(src[0].Get(), 10);
    EXPECT_EQ(src[4].Get(), 50);

    src[0].Clear();
    src[4].Clear();
    dst[2].Clear();
    dst[3].Clear();
    dst[4].Clear();
}

TEST(TValueHolder, HolderOrValueMoveBackwardWithTrackedObject)
{
    TTrackedObject::ResetActions();

    TValueHolder<TTrackedObject> src[2];
    src[0].Set(TTrackedObject(10));
    src[1].Set(TTrackedObject(20));

    TValueHolder<TTrackedObject> dst[2];

    TTrackedObject::ResetActions();
    HolderOrValueMoveBackward(src, src + 2, dst + 2);

    EXPECT_EQ(dst[0].Get().Value, 10);
    EXPECT_EQ(dst[1].Get().Value, 20);

    TTrackedObject::CheckActions({
        EActionType::MoveConstructor,
        EActionType::Destructor,
        EActionType::MoveConstructor,
        EActionType::Destructor,
    });

    dst[0].Clear();
    dst[1].Clear();
}

TEST(TValueHolder, HolderOrValueMoveBackwardOverlappingRanges)
{
    // Test moving within the same array (backward direction)
    TValueHolder<int> arr[5];
    arr[0].Set(10);
    arr[1].Set(20);
    arr[2].Set(30);

    // Move [0, 3) to [2, 5)
    HolderOrValueMoveBackward(arr, arr + 3, arr + 5);

    EXPECT_EQ(arr[2].Get(), 10);
    EXPECT_EQ(arr[3].Get(), 20);
    EXPECT_EQ(arr[4].Get(), 30);

    arr[2].Clear();
    arr[3].Clear();
    arr[4].Clear();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolder, HolderOrValueMoveRegularTypeEmptyRange)
{
    int src[3] = {10, 20, 30};
    int dst[3] = {0, 0, 0};

    // Move zero elements
    HolderOrValueMove(src, src, dst);

    EXPECT_EQ(dst[0], 0);
    EXPECT_EQ(dst[1], 0);
    EXPECT_EQ(dst[2], 0);
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeSingleElement)
{
    int src[1] = {42};
    int dst[1] = {0};

    HolderOrValueMove(src, src + 1, dst);

    EXPECT_EQ(dst[0], 42);
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeMultipleElements)
{
    int src[3] = {10, 20, 30};
    int dst[3] = {0, 0, 0};

    HolderOrValueMove(src, src + 3, dst);

    EXPECT_EQ(dst[0], 10);
    EXPECT_EQ(dst[1], 20);
    EXPECT_EQ(dst[2], 30);
}

TEST(TValueHolder, HolderOrValueMoveRegularTypePartialRange)
{
    int src[5] = {10, 20, 30, 40, 50};
    int dst[3] = {0, 0, 0};

    // Move only elements [1, 4) -> [0, 3)
    HolderOrValueMove(src + 1, src + 4, dst);

    EXPECT_EQ(dst[0], 20);
    EXPECT_EQ(dst[1], 30);
    EXPECT_EQ(dst[2], 40);

    EXPECT_EQ(src[0], 10);
    EXPECT_EQ(src[4], 50);
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeWithString)
{
    std::string src[3] = {"first", "second", "third"};
    std::string dst[3];

    HolderOrValueMove(src, src + 3, dst);

    EXPECT_EQ(dst[0], "first");
    EXPECT_EQ(dst[1], "second");
    EXPECT_EQ(dst[2], "third");
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeWithTrackedObject)
{
    TTrackedObject::ResetActions();

    TTrackedObject src[2] = {TTrackedObject(10), TTrackedObject(20)};
    TTrackedObject dst[2];

    TTrackedObject::ResetActions();
    HolderOrValueMove(src, src + 2, dst);

    EXPECT_EQ(dst[0].Value, 10);
    EXPECT_EQ(dst[1].Value, 20);

    TTrackedObject::CheckActions({
        EActionType::MoveAssignment,
        EActionType::MoveAssignment,
    });
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeMoveOverlappingRanges)
{
    // Test moving within the same array (forward direction)
    int arr[5] = {10, 20, 30, 40, 50};

    // Move [2, 5) to [0, 3)
    HolderOrValueMove(arr + 2, arr + 5, arr);

    EXPECT_EQ(arr[0], 30);
    EXPECT_EQ(arr[1], 40);
    EXPECT_EQ(arr[2], 50);
    EXPECT_EQ(arr[3], 40);
    EXPECT_EQ(arr[4], 50);
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeWithUniquePtr)
{
    std::unique_ptr<int> src[2];
    src[0] = std::make_unique<int>(42);
    src[1] = std::make_unique<int>(84);

    std::unique_ptr<int> dst[2];

    HolderOrValueMove(src, src + 2, dst);

    EXPECT_EQ(*dst[0], 42);
    EXPECT_EQ(*dst[1], 84);
    EXPECT_EQ(src[0], nullptr);
    EXPECT_EQ(src[1], nullptr);
}

TEST(TValueHolder, HolderOrValueMoveRegularTypeWithVector)
{
    std::vector<int> src[2];
    src[0] = {1, 2, 3};
    src[1] = {4, 5, 6};

    std::vector<int> dst[2];

    HolderOrValueMove(src, src + 2, dst);

    EXPECT_EQ(dst[0], (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(dst[1], (std::vector<int>{4, 5, 6}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeEmptyRange)
{
    int src[3] = {10, 20, 30};
    int dst[3] = {0, 0, 0};

    // Move zero elements
    HolderOrValueMoveBackward(src, src, dst);

    EXPECT_EQ(dst[0], 0);
    EXPECT_EQ(dst[1], 0);
    EXPECT_EQ(dst[2], 0);
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeSingleElement)
{
    int src[1] = {42};
    int dst[1] = {0};

    HolderOrValueMoveBackward(src, src + 1, dst + 1);

    EXPECT_EQ(dst[0], 42);
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeMultipleElements)
{
    int src[3] = {10, 20, 30};
    int dst[3] = {0, 0, 0};

    HolderOrValueMoveBackward(src, src + 3, dst + 3);

    EXPECT_EQ(dst[0], 10);
    EXPECT_EQ(dst[1], 20);
    EXPECT_EQ(dst[2], 30);
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypePartialRange)
{
    int src[5] = {10, 20, 30, 40, 50};
    int dst[5] = {0, 0, 0, 0, 0};

    // Move elements [1, 4) backward to positions [2, 5)
    HolderOrValueMoveBackward(src + 1, src + 4, dst + 5);

    EXPECT_EQ(dst[2], 20);
    EXPECT_EQ(dst[3], 30);
    EXPECT_EQ(dst[4], 40);

    EXPECT_EQ(src[0], 10);
    EXPECT_EQ(src[4], 50);
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeWithString)
{
    std::string src[3] = {"first", "second", "third"};
    std::string dst[3];

    HolderOrValueMoveBackward(src, src + 3, dst + 3);

    EXPECT_EQ(dst[0], "first");
    EXPECT_EQ(dst[1], "second");
    EXPECT_EQ(dst[2], "third");
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeWithTrackedObject)
{
    TTrackedObject::ResetActions();

    TTrackedObject src[2] = {TTrackedObject(10), TTrackedObject(20)};
    TTrackedObject dst[2];

    TTrackedObject::ResetActions();
    HolderOrValueMoveBackward(src, src + 2, dst + 2);

    EXPECT_EQ(dst[0].Value, 10);
    EXPECT_EQ(dst[1].Value, 20);

    TTrackedObject::CheckActions({
        EActionType::MoveAssignment,
        EActionType::MoveAssignment,
    });
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeOverlappingRanges)
{
    // Test moving within the same array (backward direction)
    int arr[5] = {10, 20, 30, 0, 0};

    // Move [0, 3) to [2, 5)
    HolderOrValueMoveBackward(arr, arr + 3, arr + 5);

    EXPECT_EQ(arr[2], 10);
    EXPECT_EQ(arr[3], 20);
    EXPECT_EQ(arr[4], 30);
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeWithUniquePtr)
{
    std::unique_ptr<int> src[2];
    src[0] = std::make_unique<int>(42);
    src[1] = std::make_unique<int>(84);

    std::unique_ptr<int> dst[2];

    HolderOrValueMoveBackward(src, src + 2, dst + 2);

    EXPECT_EQ(*dst[0], 42);
    EXPECT_EQ(*dst[1], 84);
    EXPECT_EQ(src[0], nullptr);
    EXPECT_EQ(src[1], nullptr);
}

TEST(TValueHolder, HolderOrValueMoveBackwardRegularTypeWithVector)
{
    std::vector<int> src[2];
    src[0] = {1, 2, 3};
    src[1] = {4, 5, 6};

    std::vector<int> dst[2];

    HolderOrValueMoveBackward(src, src + 2, dst + 2);

    EXPECT_EQ(dst[0], (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(dst[1], (std::vector<int>{4, 5, 6}));
}

////////////////////////////////////////////////////////////////////////////////
