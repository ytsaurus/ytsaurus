# Lock-free map

Набор thread-safe lock-free ассоциативных контейнеров, реализующих интерфейсы set/map. Используют алгоритм Harris-Michael.

**Ссылки на статьи:**
- [A pragmatic implementation of non-blocking linked-lists](https://www.cl.cam.ac.uk/research/srg/netos/papers/2001-caslists.pdf)
- [High performance dynamic lock-free hash tables](https://docs.rs/crate/crossbeam/0.2.4/source/hash-and-skip.pdf)

## **THarrisMichaelSet/Map**

**Реализация:** Хеш-таблица с фиксированным количеством бакетов (`NumOfBuckets`). Каждый бакет - lock-free отсортированный список.

**Шаблонные параметры:**
- `TValue` (Set) / `TKey, TValue` (Map)
- `NumOfBuckets`: количество бакетов
- `TOptions...` (опционально):
  - `NOptions::THasher`: хеш-функция (по умолчанию `THash`)
  - `NOptions::TCompare`: компаратор (по умолчанию `TLess`)
  - `NOptions::TBackoff`: стратегия ожидания (см. ниже)
  - `NOptions::TReclaimer`: стратегия управления памятью (см. ниже)

**Интерфейс:**
```cpp
explicit THarrisMichaelSet(const hasher& = {}, const key_compare& = {}, const TReclaimer& = {});

bool insert(const value_type&);
bool insert(value_type&&);
template<typename T> bool insert(T&&);
template<typename... Args> bool emplace(Args&&...);

bool erase(const key_type&);

TAccessor extract(const key_type&); // Возвращает извлечённый и защищённый элемент.

TAccessor at(const key_type&);
TConstAccessor at(const key_type&) const;

iterator find(const key_type&);
const_iterator find(const key_type&) const;
bool contains(const key_type&) const;

size_t size() const;
bool empty() const;

void clear();

iterator begin();
iterator end();
const_iterator cbegin() const;
const_iterator cend() const;
const_iterator begin() const;
const_iterator end() const;
```

{% note warning %}

Итераторы хранят `THazardPointer`, что ограничивает их количество в одном потоке. Рекомендуется перемещать (move) итераторы вместо копирования.

{% endnote %}

**Пример:**

```cpp
using namespace NLockFreeMap;

THarrisMichaelSet<T, 128, NOptions::THasher<MyHash>, NOptions::TBackoff<TExponentialBackoff<8>>> set;
```

## **THarrisMichaelListSet/Map**

**Реализация:** lock-free отсортированный связный список.

**Шаблонные параметры:**
- `TValue` (Set) / `TKey, TValue` (Map)
- `TOptions...` (опционально):
  - `NOptions::TCompare`: компаратор (по умолчанию `TLess`)
  - `NOptions::TBackoff`: стратегия ожидания
  - `NOptions::TReclaimer`: стратегия управления памятью (см. ниже)

**Интерфейс:**
```cpp
// Все методы THarrisMichaelSet/Map +
iterator lower_bound(const key_type&);
const_iterator lower_bound(const key_type&) const;
```

{% note warning %}

Итераторы хранят `THazardPointer`, что ограничивает их количество в одном потоке. Рекомендуется перемещать (move) итераторы вместо копирования.

{% endnote %}

**Пример:**

```cpp
THarrisMichaelListSet<T, NOptions::TBackoff<TExponentialBackoff<8>>> set;
```

## Стратегии **Backoff**

Параметр `TBackoff` управляет поведением при конфликтах:
- `TNoneBackoff`: немедленная повторная попытка
- `TYieldBackoff`: `std::this_thread::yield()`
- `TPauseBackoff`: инструкция `pause` (оптимизировано для x86)
- `TExponentialBackoff`: экспоненциальное увеличение паузы

## Стратегия управления памятью **Reclaimer**

На текущий момент реализована единственная стратегия - `THazardPointerReclaimer`. Однако у нее есть возможность указывать конкретный `THazardPointerDomain`. По дефолту используется `NHp::GetDefaultDomain()`

Пример:

```cpp
using namespace NLockFreeMap;

NHp::THazardPointerDomain customDomain(10, 32, 10);

THarrisMichaelListSet<std::size_t> set({}, THazardPointerReclaimer(customDomain));

for (std::size_t i = 0; i < 10; ++i) {
    set.emplace(i);
}
```

## **Perfomance**

![text](imgs/containers_comparison.png)