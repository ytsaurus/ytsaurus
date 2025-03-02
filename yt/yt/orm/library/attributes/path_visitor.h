#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/stack.h>
#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

// A cookie explaining how the decision to call a ProtoVisitor method was made.
DEFINE_ENUM(EVisitReason,
    (TopLevel)  // Visiting the message supplied by the caller.
    (Path)      // Visiting an object indicated by the path.
    (Asterisk)  // Visiting all entries indicated by an asterisk.
    (AfterPath) // Visiting the entire subtree after exhausting the path.
    (Manual)    // Visit out of the ordinary pattern initiated by the implementation.
);

// Mixin class for managing the current path
class TPathVisitorMixin
{
    template <typename TSelf>
    friend class TPathVisitor;

public:
    /// Policy flags.
    // Allows a "fragment" path missing the leading slash. COMPAT.
    DEFINE_BYVAL_RW_PROPERTY(bool, LeadingSlashOptional, false);
    // Having reached the end of the tokenizer path, visit everything in the field/map/repeated.
    // Does not throw when visiting absent fields.
    DEFINE_BYVAL_RW_PROPERTY(bool, VisitEverythingAfterPath, false);
    // Do not throw if the path leads into a missing field/key/index.
    DEFINE_BYVAL_RW_PROPERTY(EMissingFieldPolicy, MissingFieldPolicy, EMissingFieldPolicy::Throw);
    // Visit all fields/entries when the path has a "*".
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowAsterisk, false);

protected:
    /// Control flags.
    // Breaks out of asterisk and afterpath loops for the rest of the visit.
    DEFINE_BYVAL_RW_PROPERTY(bool, StopIteration, false);

    /// Tokenizer management.
    // Advances tokenizer over a slash unless it's optional here.
    void SkipSlash();

    using TToken = std::variant<int, ui64, TStringBuf>;

    // Pushes the token onto the current path.
    void Push(TToken token);

    // Advances the tokenizer and pushes the token onto the current path.
    void AdvanceOver(TToken token);

    // Throws if we don't support asterisks. Advances the tokenizer.
    void AdvanceOverAsterisk();

    class TCheckpoint
    {
    public:
        explicit TCheckpoint(NYPath::TTokenizer& tokenizer);
        ~TCheckpoint();

        void Defer(std::function<void()> defer);

    private:
        NYPath::TTokenizer::TCheckpoint TokenizerCheckpoint_;
        std::function<void()> Defer_;
    };

    // Called when visiting a path that can branch (asterisks and "after path" traversals).
    // Pushes the token onto the current path.
    // Checkpoint destructor restores the tokenizer and current path.
    TCheckpoint CheckpointBranchedTraversal(TToken token);

    // Methods for examining the tokenizer and the stack.
    NYPath::ETokenType GetTokenizerType() const;
    TStringBuf GetTokenizerInput() const;
    TStringBuf GetToken() const;
    const TString& GetLiteralValue() const;
    const NYPath::TYPath& GetCurrentPath() const;

    // Throws if the token type is wrong.
    void Expect(NYPath::ETokenType type) const;
    // Returns true if the tokenizer has completed the path.
    bool PathComplete() const;
    // Prepares working set for a new traversal.
    void Reset(NYPath::TYPathBuf path);

    /// Index management.
    // Computes the repeated field index from the current token.
    TErrorOr<TIndexParseResult> ParseCurrentListIndex(int size) const;

private:
    // Maintains the path supplied by the caller.
    NYPath::TTokenizer Tokenizer_;
    // Maintains the current path of the visitor. Will differ from the path in the Tokenizer:
    // - When traversing asterisks and items after the path (actual keys are substituted),
    // - When traversing negative and relative indices (actual indices are substituted),
    // - When manipulated by the concrete visitor (it is recommended to maintain the path when
    //   manually controlling the visitor).
    NYPath::TYPathStack CurrentPath_;
};

////////////////////////////////////////////////////////////////////////////////

/// Construction kit for pain-free (hopefully) path-guided traversals.
//
// 1. Make your own visitor by subclassing a CRTP specialization of TPathVisitor and also
//    TPathVisitorMixin. Make it final to let the compiler inline stuff.
//
// 2. Set policy flags of TPathVisitorMixin in the constructor or at call site.
//
// 3. Override (by hiding) the methods that handle structures that are relevant to your task. As a
//    general pattern, try to handle a situation (say, PathComplete says you've reached your
//    destination) and fall back to the base implementation. Note that the visitor is recursive, so
//    not calling the base stops the visit of the current subtree.
//
// 4. Befriend TPathVisitor and/or add "using TPathVisitor::Self" if the compiler complains.
//
// 5. Feel free to use utilities in the base.
//
// Tokenizer_ is always advanced after a token is converted into something that will be passed into
// the next method (the next message, an index or a key). Expect every method to be at the next /token
// or at PathComplete.
//
// 5. Call Visit with the structure you want to visit.

/// Design notes.
//
// The visitor is recursive because the language will manage a DFS stack much better than us.
//
// We're using CRTP. We're losing "override" safety and any call you make without "Self()->" is no
// longer "static virtual" (unless you're in a final class). Be careful.
//
// The methods try to be obvious, but you'll probably end up examining the base implementation to
// see what is and is not done for you. You probably want to at least handle terminals wherever we
// Throw Unimplemented.
//
// The base implementation provides for directed traversal of a path in a tree of vectors and maps.
//
// There is rudimentary support for populating missing path entries with the |Force| missing field
// policy.
//
// The use case is writing generic code in ORM attributes that calls Visit and just works.

template <typename TSelf>
class TPathVisitor
{
public:
    template <typename TVisitParam>
    void Visit(TVisitParam&& target, NYPath::TYPathBuf path);

protected:
    // Curiously Recurring Template Pattern.
    TSelf* Self();
    const TSelf* Self() const;

    // Container router.
    template <typename TVisitParam>
    void VisitGeneric(TVisitParam&& target, EVisitReason reason);

    // Vector section.
    // The parameter is a vector.
    template <typename TVisitParam>
    void VisitVector(TVisitParam&& target, EVisitReason reason);

    // Called for asterisks and visits after the path.
    template <typename TVisitParam>
    void VisitWholeVector(TVisitParam&& target, EVisitReason reason);

    // The path called for an absolute index in a vector.
    template <typename TVisitParam>
    void VisitVectorEntry(TVisitParam&& target, int index, EVisitReason reason);

    // The path called for a relative index (insertion before the index) in a vector.
    template <typename TVisitParam>
    void VisitVectorEntryRelative(TVisitParam&& target, int index, EVisitReason reason);

    /// Called when the index is out of bounds. Throws unless missing entries are allowed.
    template <typename TVisitParam>
    void OnVectorIndexError(TVisitParam&& target, EVisitReason reason, TError error);

    // Map section.
    // The parameter is a map.
    template <typename TVisitParam>
    void VisitMap(TVisitParam&& target, EVisitReason reason);

    // Called for asterisks and visits after the path.
    template <typename TVisitParam>
    void VisitWholeMap(TVisitParam&& target, EVisitReason reason);

    // The key was found in the map.
    template <typename TVisitParam, typename TMapIterator>
    void VisitMapEntry(
        TVisitParam&& target,
        TMapIterator mapIterator,
        TString key,
        EVisitReason reason);

    // The key was not found in the map.
    template <typename TVisitParam, typename TMapKey>
    void OnMapKeyError(TVisitParam&& target, TMapKey mapKey, TString key, EVisitReason reason);

    // Other section.
    // The parameter is any other C++ type.
    template <typename TVisitParam>
    void VisitOther(TVisitParam&& target, EVisitReason reason);
}; // TPathVisitor

} // namespace NYT::NOrm::NAttributes

#define PATH_VISITOR_INL_H_
#include "path_visitor-inl.h"
#undef PATH_VISITOR_INL_H_
