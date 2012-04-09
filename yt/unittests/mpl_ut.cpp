#include "stdafx.h"

#include "../ytlib/misc/mpl.h"

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Auxiliary types.
struct TSomeStruct { };
class TSomeClass { };
enum TSomeEnum { };

class TParent
{ };

class TChild : public TParent
{ };

class TChildWithRC
{
    void Ref();
    void Unref();
};

// A class with specialization for types derived from TParent.
template <class T, class = void>
class TWrapped
{
public:
    int CleverTemplateFunction()
    {
        return 17;
    }
};

template <class T>
class TWrapped<T, typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T*, TParent*>
    >::TType>
{
public:
    int CleverTemplateFunction()
    {
        return 42;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TMetaProgrammingTest, IsSame)
{
    EXPECT_IS_TRUE (( NMpl::TIsSame<void, void>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsSame<int, int>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsSame<int*, int*>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsSame<int&, int&>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsSame<char, int>::Value ));
}

TEST(TMetaProgrammingTest, Conditional)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.other/conditional.pass.cpp?view=markup
    EXPECT_IS_TRUE(( NMpl::TIsSame<
        NMpl::TConditional<true,  char, int>::TType,
        char
    >::Value ));
    EXPECT_IS_TRUE(( NMpl::TIsSame<
        NMpl::TConditional<false, char, int>::TType,
        int
    >::Value ));
}

TEST(TMetaProgrammingTest, IsConst)
{
    EXPECT_IS_FALSE(( NMpl::TIsConst<int>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsConst<int const>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConst<int volatile>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsConst<int const volatile>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsConst<int* const>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConst<int const*>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConst<int const&>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConst<int*>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConst<int&>::Value ));
}

TEST(TMetaProgrammingTest, IsVolatile)
{
    EXPECT_IS_FALSE(( NMpl::TIsVolatile<int>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsVolatile<int const>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsVolatile<int volatile>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsVolatile<int const volatile>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsVolatile<int* volatile>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsVolatile<int volatile*>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsVolatile<int volatile&>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsVolatile<int*>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsVolatile<int&>::Value ));
}

TEST(TMetaProgrammingTest, RemoveConst)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.cv/remove_const.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TRemoveConst<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(const void, void);
    EXPECT_HERE(volatile void, volatile void);
    EXPECT_HERE(const volatile void, volatile void);

    EXPECT_HERE(int, int);
    EXPECT_HERE(int const, int);
    EXPECT_HERE(int volatile, int volatile);
    EXPECT_HERE(int const volatile, int volatile);

    EXPECT_HERE(int[3], int[3]);
    EXPECT_HERE(const int[3], int[3]);
    EXPECT_HERE(volatile int[3], volatile int[3]);
    EXPECT_HERE(const volatile int[3], volatile int[3]);
  
    EXPECT_HERE(int&, int&);
    EXPECT_HERE(const int&, const int&);

    EXPECT_HERE(int&&, int&&);
    EXPECT_HERE(const int&&, const int&&);
 
    EXPECT_HERE(int*, int*);
    EXPECT_HERE(int* const, int*);
    EXPECT_HERE(int* volatile, int* volatile);
    EXPECT_HERE(int* const volatile, int* volatile);

    EXPECT_HERE(const int*, const int*);
    EXPECT_HERE(const int* const, const int*);
    EXPECT_HERE(const int* volatile, const int* volatile);
    EXPECT_HERE(const int* const volatile, const int* volatile);
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, RemoveVolatile)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.cv/remove_volatile.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TRemoveVolatile<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(const void, const void);
    EXPECT_HERE(volatile void, void);
    EXPECT_HERE(const volatile void, const void);

    EXPECT_HERE(int, int);
    EXPECT_HERE(int const, int const);
    EXPECT_HERE(int volatile, int);
    EXPECT_HERE(int const volatile, int const);

    EXPECT_HERE(int[3], int[3]);
    EXPECT_HERE(const int[3], const int[3]);
    EXPECT_HERE(volatile int[3], int[3]);
    EXPECT_HERE(const volatile int[3], const int[3]);
  
    EXPECT_HERE(int&, int&);
    EXPECT_HERE(const int&, const int&);

    EXPECT_HERE(int&&, int&&);
    EXPECT_HERE(const int&&, const int&&);
 
    EXPECT_HERE(int*, int*);
    EXPECT_HERE(int* const, int* const);
    EXPECT_HERE(int* volatile, int*);
    EXPECT_HERE(int* const volatile, int* const);

    EXPECT_HERE(volatile int*, volatile int*);
    EXPECT_HERE(volatile int* const, volatile int* const);
    EXPECT_HERE(volatile int* volatile, volatile int*);
    EXPECT_HERE(volatile int* const volatile, volatile int* const);
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, RemoveCV)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.cv/remove_cv.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TRemoveCV<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(const void, void);
    EXPECT_HERE(volatile void, void);
    EXPECT_HERE(const volatile void, void);

    EXPECT_HERE(int, int);
    EXPECT_HERE(int const, int);
    EXPECT_HERE(int volatile, int);
    EXPECT_HERE(int const volatile, int);

    EXPECT_HERE(int[3], int[3]);
    EXPECT_HERE(const int[3], int[3]);
    EXPECT_HERE(volatile int[3], int[3]);
    EXPECT_HERE(const volatile int[3], int[3]);
  
    EXPECT_HERE(int&, int&);
    EXPECT_HERE(const int&, const int&);

    EXPECT_HERE(int&&, int&&);
    EXPECT_HERE(const int&&, const int&&);
 
    EXPECT_HERE(int*, int*);
    EXPECT_HERE(int* const, int*);
    EXPECT_HERE(int* volatile, int*);
    EXPECT_HERE(int* const volatile, int*);

    EXPECT_HERE(const int*, const int*);
    EXPECT_HERE(const int* const, const int*);
    EXPECT_HERE(const int* volatile, const int*);
    EXPECT_HERE(const int* const volatile, const int*);

    EXPECT_HERE(volatile int*, volatile int*);
    EXPECT_HERE(volatile int* const, volatile int*);
    EXPECT_HERE(volatile int* volatile, volatile int*);
    EXPECT_HERE(volatile int* const volatile, volatile int*);
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, IsReference)
{
    EXPECT_IS_TRUE (( NMpl::TIsReference<int&>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsReference<int&&>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsReference<int const&>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsReference<int const&&>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsReference<int (&)(long)>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsReference<int (&&)(long)>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsReference<int>::Value));
}

TEST(TMetaProgrammingTest, IsLvalueReference)
{
    EXPECT_IS_TRUE (( NMpl::TIsLvalueReference<int&>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsLvalueReference<int&&>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsLvalueReference<int const&>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsLvalueReference<int const&&>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsLvalueReference<int (&)(long)>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsLvalueReference<int (&&)(long)>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsLvalueReference<int>::Value));
}

TEST(TMetaProgrammingTest, IsRvalueReference)
{
    EXPECT_IS_FALSE(( NMpl::TIsRvalueReference<int&>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsRvalueReference<int&&>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsRvalueReference<int const&>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsRvalueReference<int const&&>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsRvalueReference<int (&)(long)>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsRvalueReference<int (&&)(long)>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsRvalueReference<int>::Value));
}

TEST(TMetaProgrammingTest, RemoveReference)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.ref/remove_ref.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TRemoveReference<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(int, int);
    EXPECT_HERE(int[3], int[3]);
    EXPECT_HERE(int*, int*);
    EXPECT_HERE(const int*, const int*);

    EXPECT_HERE(int&, int);
    EXPECT_HERE(const int&, const int);
    EXPECT_HERE(int(&)[3], int[3]);
    EXPECT_HERE(int*&, int*);
    EXPECT_HERE(const int*&, const int*);

    EXPECT_HERE(int&&, int);
    EXPECT_HERE(const int&&, const int);
    EXPECT_HERE(int(&&)[3], int[3]);
    EXPECT_HERE(int*&&, int*);
    EXPECT_HERE(const int*&&, const int*)
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, AddLvalueReference)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.ref/add_lvalue_ref.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TAddLvalueReference<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(int, int&);
    EXPECT_HERE(int[3], int(&)[3]);
    EXPECT_HERE(int&, int&);
    EXPECT_HERE(const int&, const int&);
    EXPECT_HERE(int*, int*&);
    EXPECT_HERE(const int*, const int*&);
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, AddRvalueReference)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.ref/add_rvalue_ref.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TAddRvalueReference<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(int, int&&);
    EXPECT_HERE(int[3], int(&&)[3]);
    EXPECT_HERE(int&, int&);
    EXPECT_HERE(const int&, const int&);
    EXPECT_HERE(int*, int*&&);
    EXPECT_HERE(const int*, const int*&&);
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, RemoveExtent)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.arr/remove_extent.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TRemoveExtent<A>::TType, B>::Value ));

    EXPECT_HERE(int, int);
    EXPECT_HERE(const TSomeEnum, const TSomeEnum);
    EXPECT_HERE(int[], int);
    EXPECT_HERE(const int[], const int);
    EXPECT_HERE(int[3], int);
    EXPECT_HERE(const int[3], const int);
    EXPECT_HERE(int[][3], int[3]);
    EXPECT_HERE(const int[][3], const int[3]);
    EXPECT_HERE(int[2][3], int[3]);
    EXPECT_HERE(const int[2][3], const int[3]);
    EXPECT_HERE(int[1][2][3], int[2][3]);
    EXPECT_HERE(const int[1][2][3], const int[2][3]);
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, Decay)
{
    // http://llvm.org/viewvc/llvm-project/libcxx/trunk/test/utilities/meta/meta.trans/meta.trans.other/decay.pass.cpp?view=markup
#define EXPECT_HERE(A, B) \
    EXPECT_IS_TRUE (( NMpl::TIsSame<typename NMpl::TDecay<A>::TType, B>::Value ));

    EXPECT_HERE(void, void);
    EXPECT_HERE(int, int);
    EXPECT_HERE(const volatile int, int);
    EXPECT_HERE(int*, int*);
    EXPECT_HERE(int[], int*);
    EXPECT_HERE(int[3], int*);
    EXPECT_HERE(const int[], const int*);
    EXPECT_HERE(const int[3], const int*);
    // To keep implementation simple, function decaying was not implemented.
    // EXPECT_HERE(void(), void (*)());
#undef EXPECT_HERE
}

TEST(TMetaProgrammingTest, IsConvertible)
{
    EXPECT_IS_TRUE (( NMpl::TIsConvertible<TChild, TParent>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConvertible<TParent, TChild>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsConvertible<TChild*, TParent*>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConvertible<TParent*, TChild*>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsConvertible<TChild*, TChild*>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsConvertible<TParent*, TParent*>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsConvertible<TParent, TSomeStruct>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConvertible<TParent, TSomeClass>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConvertible<TParent, TSomeEnum>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsConvertible<int, float>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsConvertible<int, double>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsConvertible<int*, void*>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConvertible<void*, int*>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsConvertible<int[17], double>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsConvertible<double, int[17]>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsConvertible<int[17], int*>::Value ));
}

TEST(TMetaProgrammingTest, IsClass)
{
    EXPECT_IS_FALSE(( NMpl::TIsClass<void>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsClass<int>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsClass<int*>::Value ));

    EXPECT_IS_TRUE (( NMpl::TIsClass<TSomeStruct>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsClass<TSomeClass>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsClass<TSomeEnum>::Value ));
}

TEST(TMetaProgrammingTest, IsPod)
{
    EXPECT_IS_TRUE (( NMpl::TIsPod<char>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsPod<int>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsPod<short>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsPod<long>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsPod<float>::Value ));
    EXPECT_IS_TRUE (( NMpl::TIsPod<double>::Value ));

    EXPECT_IS_FALSE(( NMpl::TIsPod<TSomeStruct>::Value ));
    EXPECT_IS_FALSE(( NMpl::TIsPod<TSomeClass>::Value ));
}

TEST(TMetaProgrammingTest, EnableIf)
{
    EXPECT_EQ(17, TWrapped<int>().CleverTemplateFunction());
    EXPECT_EQ(17, TWrapped<float>().CleverTemplateFunction());
    EXPECT_EQ(42, TWrapped<TParent>().CleverTemplateFunction());
    EXPECT_EQ(42, TWrapped<TChild>().CleverTemplateFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace <anonymous>
} // namespace NYT
