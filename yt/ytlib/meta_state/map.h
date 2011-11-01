#pragma once

#include "common.h"

#include "../misc/enum.h"
#include "../misc/assert.h"
#include "../misc/foreach.h"
#include "../misc/serialize.h"
#include "../misc/thread_affinity.h"

#include <util/ysaveload.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// TODO: Create inl-file and move implementation there

// TODO: DECLARE_ENUM cannot be used in a template class.
class TMetaStateMapBase
    : private TNonCopyable
{
protected:
    /*!
     * Transitions
     * - Normal -> LoadingSnapshot,
     * - Normal -> SavingSnapshot,
     * - HasPendingChanges -> Normal
     * are performed from the user thread.
     *
     * Transitions
     * - LoadingSnapshot -> Normal,
     * - SavingSnapshot -> HasPendingChanges
     * are performed from the snapshot invoker.
     */
    DECLARE_ENUM(EState,
        (Normal)
        (LoadingSnapshot)
        (SavingSnapshot)
        (HasPendingChanges)
    );

    EState State;
};

template <class TValue>
struct TDefaultMetaMapTraits
{
    TAutoPtr<TValue> Clone(TValue* value) const;

    void Save(TValue* value, TOutputStream* output) const;
    TAutoPtr<TValue> Load(TInputStream* input) const;
};

//! Snapshottable map used to store various meta-state tables.
/*!
 *  \tparam TKey Key type.
 *  \tparam TValue Value type.
 *  \tparam THash Hash function for keys.
 * 
 *  \note
 *  All public methods must be called from a single thread.
 *  Exceptions are #Begin and #End, see below.
 * 
 *  TODO: this is not true, write about Traits
 *  TValue type must have the following methods:
 *          TAutoPtr<TValue> Clone();
 *          void Save(TOutputStream* output);
 *          static TAutoPtr<TValue> Load(TInputStream* input);
 */
template <
    class TKey,
    class TValue,
    class TTraits = TDefaultMetaMapTraits<TValue>,
    class THash = ::THash<TKey>
>
class TMetaStateMap
    : protected TMetaStateMapBase
{
public:
    typedef TMetaStateMap<TKey, TValue, TTraits, THash> TThis;
    typedef yhash_map<TKey, TValue*, THash> TMap;
    typedef typename TMap::iterator TIterator;
    typedef typename TMap::iterator TConstIterator;

    explicit TMetaStateMap(TTraits traits = TTraits());

    ~TMetaStateMap();

    //! Inserts a key-value pair.
    /*!
     *  \param key A key to insert.
     *  \param value A value to insert.
     *  
     *  \note The map will own the value and will call "delete" for it  when time comes.
     *  \note Fails if the key is already in map.
     */
    void Insert(const TKey& key, TValue* value);

    //! Tries to find a value by its key. The returned value is read-only.
    /*!
     *  \param key A key.
     *  \return A pointer to the value if found, NULL otherwise.
     */
    const TValue* Find(const TKey& key) const;

    //! Tries to find a value by its key.
    //! May return a modifiable copy if snapshot creation is in progress.
    /*!
     * \param key A key.
     * \return A pointer to the value if found, NULL otherwise.
     */
    TValue* FindForUpdate(const TKey& key);

    //! Returns a read-only value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns A reference to the value.
     */
    const TValue& Get(const TKey& key) const;

    //! Returns a modifiable value corresponding to the key.
    /*!
     *  In contrast to #Find this method fails if the key does not exist in the map.
     *  \param key A key.
     *  \returns A reference to the value.
     */
    TValue& GetForUpdate(const TKey& key);

    //! Removes the key from the map and deletes the corresponding value.
    /*!
     *  \param A key.
     *  
     *  \note Fails if the key is not in the map.
     */
    void Remove(const TKey& key);

    //! Checks whether the key exists in the map.
    /*!
     *  \param key A key.
     *  \return True iff the key exists in the map.
     */
    bool Contains(const TKey& key) const;

    //! Clears the map.
    void Clear();

    //! Returns the size of the map.
    int GetSize() const;

    yvector<TKey> GetKeys() const;

    //! (Unordered) begin()-iterator.
    /*!
     *  \note
     *  This call is potentially dangerous! 
     *  The user must understand its semantics and call it at its own risk.
     *  Iteration is only possible when no snapshot is being created.
     *  A typical use-case is to iterate over the items right after reading a snapshot.
     */
    TIterator Begin();

    //! (Unordered) end()-iterator.
    /*!
     *  See the note for #Begin.
     */
    TIterator End();
    
    //! (Unordered) const begin()-iterator.
    /*!
     *  See the note for #Begin.
     */
    TConstIterator Begin() const;

    //! (Unordered) const end()-iterator.
    /*!
     *  See the note for #Begin.
     */
    TConstIterator End() const;

    //! Asynchronously saves the map to the stream.
    /*!
     *  This method saves the snapshot of the map as it is seen at the moment of
     *  the invocation. All further updates are accepted but are kept in-memory.
     *  
     *  \param invoker Invoker used to perform the heavy lifting.
     *  \param stream Output stream.
     *  \return An asynchronous result indicating that the snapshot is saved.
     */
    TFuture<TVoid>::TPtr Save(IInvoker::TPtr invoker, TOutputStream* output);

    //! Asynchronously loads the map from the stream.
    /*!
     * This method loads the snapshot of the map in the background and at some
     * moment in the future swaps current map with the loaded one.
     * \param invoker Invoker for actual heavy work.
     * \param stream Input stream.
     * \return Callback on successful load.
     */
    TFuture<TVoid>::TPtr Load(IInvoker::TPtr invoker, TInputStream* input);
    
private:
    DECLARE_THREAD_AFFINITY_SLOT(UserThread);
    
    //! When no shapshot is being written this is the actual map we're working with.
    //! When a snapshot is being created this map is kept read-only and
    //! #PathMap is used to store the changes.
    TMap PrimaryMap;

    //! "(key, NULL)" indicates that the key should be deleted.
    TMap PatchMap;

    TTraits Traits;
    int Size;
    
    typedef TPair<TKey, TValue*> TItem;

    TVoid DoSave(TOutputStream* output);
    TVoid DoLoad(TInputStream* input);

    void MergeTempTablesIfNeeded();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT


namespace NYT {
namespace NForeach {

//! Provides a begin-like iterator for #FOREACH macro.
template<class TKey, class TValue, class THash>
inline auto Begin(NMetaState::TMetaStateMap<TKey, TValue, THash>& collection) -> decltype(collection.Begin())
{
    return collection.Begin();
}

//! Provides an end-like iterator for #FOREACH macro.
template<class TKey, class TValue, class THash>
inline auto End(NMetaState::TMetaStateMap<TKey, TValue, THash>& collection) -> decltype(collection.End())
{
    return collection.End();
}
 
} // namespace NForeach
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

#define METAMAP_ACCESSORS_DECL(entityName, entityType, idType) \
    const entityType* Find ## entityName(const idType& id) const; \
    entityType* Find ## entityName ## ForUpdate(const idType& id); \
    const entityType& Get ## entityName(const idType& id) const; \
    entityType& Get ## entityName ## ForUpdate(const idType& id); \
    yvector<idType> Get ## entityName ## Ids();

#define METAMAP_ACCESSORS_IMPL(declaringType, entityName, entityType, idType, map) \
    const entityType* declaringType::Find ## entityName(const idType& id) const \
    { \
        return (map).Find(id); \
    } \
    \
    entityType* declaringType::Find ## entityName ## ForUpdate(const idType& id) \
    { \
        return (map).FindForUpdate(id); \
    } \
    \
    const entityType& declaringType::Get ## entityName(const idType& id) const \
    { \
        return (map).Get(id); \
    } \
    \
    entityType& declaringType::Get ## entityName ## ForUpdate(const idType& id) \
    { \
        return (map).GetForUpdate(id); \
    } \
    \
    yvector<idType> declaringType::Get ## entityName ## Ids() \
    { \
        return (map).GetKeys(); \
    }

#define METAMAP_ACCESSORS_FWD(declaringType, entityName, entityType, idType, fwd) \
    const entityType* declaringType::Find ## entityName(const idType& id) const \
    { \
        return (fwd).Find ## entityName(id); \
    } \
    \
    entityType* declaringType::Find ## entityName ## ForUpdate(const idType& id) \
    { \
        return (fwd).Find ## entityName ## ForUpdate(id); \
    } \
    \
    const entityType& declaringType::Get ## entityName(const idType& id) const \
    { \
        return (fwd).Get ## entityName(id); \
    } \
    \
    entityType& declaringType::Get ## entityName ## ForUpdate(const idType& id) \
    { \
        return (fwd).Get ## entityName ## ForUpdate(id); \
    } \
    \
    yvector<idType> declaringType::Get ## entityName ## Ids() \
    { \
        return (fwd).Get ## entityName ## Ids(); \
    }

////////////////////////////////////////////////////////////////////////////////

#define MAP_INL_H_
#include "map-inl.h"
#undef MAP_INL_H_
