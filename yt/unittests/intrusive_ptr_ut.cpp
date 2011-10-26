#include "../ytlib/misc/common.h"
#include "../ytlib/misc/ref_counted_base.h"
#include "../ytlib/misc/new.h"

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
	//! This object represents simple reference-counted object.
	class TSimpleObject
		: public TRefCountedBase
	{
	public:
		typedef TIntrusivePtr<TSimpleObject> TPtr;
	};

	//! This object is derived from reference-counted one.
	class TAnotherSimpleObject
		: public TSimpleObject
	{
	public:
		typedef TIntrusivePtr<TAnotherSimpleObject> TPtr;
	};

	//! This object prohibits copy and move constructors and keeps track
	//! of number of ctor/dtor invokations.
	class TIntricateObject
		: public NYT::TRefCountedBase
	{
	public:
		typedef TIntrusivePtr<TIntricateObject> TPtr;

		TIntricateObject()
		{
			TIntricateObject::OnCtor(TPtr(this));
		}

		~TIntricateObject()
		{
			TIntricateObject::OnDtor();
		}

	private:
		TIntricateObject(const TIntricateObject&);
		TIntricateObject(const TIntricateObject&&);
		TIntricateObject& operator=(const TIntricateObject&);
		TIntricateObject& operator=(const TIntricateObject&&);

	public:
		static int CtorCalls;
		static int DtorCalls;

		static void ResetGlobalState()
		{
			CtorCalls = 0;
			DtorCalls = 0;
		}

		static void OnCtor(TPtr)
		{
			++CtorCalls;
		}

		static void OnDtor()
		{
			++DtorCalls;
		}
	};

	int TIntricateObject::CtorCalls = 0;
	int TIntricateObject::DtorCalls = 0;
} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

TEST(TIntrusivePtrTest, SingleReference)
{
	TSimpleObject* null = NULL;

	TSimpleObject::TPtr foo = New<TSimpleObject>();
	TSimpleObject::TPtr bar;

	EXPECT_NE(null, foo.Get());
	EXPECT_EQ(null, bar.Get());

	EXPECT_EQ(1, foo->GetReferenceCount());
}

TEST(TIntrusivePtrTest, DoubleReference)
{
	TSimpleObject* null = NULL;

	TSimpleObject::TPtr foo = New<TSimpleObject>();
	TSimpleObject::TPtr bar;

	EXPECT_EQ(null, bar.Get());

	bar = foo;
	EXPECT_NE(null, bar.Get());

	EXPECT_EQ(2, foo->GetReferenceCount());
	EXPECT_EQ(2, bar->GetReferenceCount());

	bar.Reset();
	EXPECT_EQ(null, bar.Get());
	EXPECT_EQ(1, foo->GetReferenceCount());
}

TEST(TIntrusivePtrTest, Swap)
{
	TSimpleObject* null = NULL;

	TSimpleObject::TPtr foo = New<TSimpleObject>();
	TSimpleObject::TPtr bar;

	EXPECT_NE(null, foo.Get());
	EXPECT_EQ(null, bar.Get());
	EXPECT_EQ(1, foo->GetReferenceCount());

	foo.Swap(bar);

	EXPECT_EQ(null, foo.Get());
	EXPECT_NE(null, bar.Get());
	EXPECT_EQ(1, bar->GetReferenceCount());

	foo.Swap(bar);

	EXPECT_NE(null, foo.Get());
	EXPECT_EQ(null, bar.Get());
	EXPECT_EQ(1, foo->GetReferenceCount());
}

TEST(TIntrusivePtrTest, Cast)
{
	TSimpleObject::TPtr foo = New<TSimpleObject>();
	TSimpleObject::TPtr bar = New<TAnotherSimpleObject>();

	EXPECT_EQ(1, foo->GetReferenceCount());
	EXPECT_EQ(1, bar->GetReferenceCount());
}

TEST(TIntrusivePtrTest, Intricate)
{
	TIntricateObject::ResetGlobalState();

	TIntricateObject::TPtr object;
	EXPECT_EQ(0, TIntricateObject::CtorCalls);
	EXPECT_EQ(0, TIntricateObject::DtorCalls);

	object = New<TIntricateObject>();

	EXPECT_EQ(1, TIntricateObject::CtorCalls);
	EXPECT_EQ(0, TIntricateObject::DtorCalls);

	object.Reset();

	EXPECT_EQ(1, TIntricateObject::CtorCalls);
	EXPECT_EQ(1, TIntricateObject::DtorCalls);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
