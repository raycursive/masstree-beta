#ifndef BINARYTREE_HH
#define BINARYTREE_HH
#include "circular_int.hh"
#include "compiler.hh"
#include "fixsizedkey.hh"
#include "kvthread.hh"
#include "mtcounters.hh"
#include "str.hh"
#include "string.hh"
#include <mutex>

namespace binarytree {
using lcdf::Str;
using lcdf::String;

template <size_t KeySize> struct tree_params {
  static constexpr int ikey_size = KeySize;
  using value_type = Str;
  using threadinfo_type = ::threadinfo;
};

// class SpinLock {
// private:
//   std::atomic_flag flag = ATOMIC_FLAG_INIT;

// public:
//   void lock() {
//     while (flag.test_and_set(std::memory_order_acquire)) {
//     }
//   }
//   void unlock() { flag.clear(std::memory_order_release); }
// };

// using scoped_lock =  std::lock_guard<SpinLock>;

template <typename P> class node;
template <typename P> class cursor;

template <typename P> class binary_tree {
public:
  using parameter_type = P;
  using node_type = node<P>;
  using value_type = typename P::value_type;
  using key_type = fix_sized_key<P::ikey_size>;
  using threadinfo = typename P::threadinfo_type;
  using cursor_type = cursor<P>;

  inline binary_tree() {}

  void initialize(threadinfo &ti);
  void destroy(threadinfo &ti);

  inline node_type *root() const { return root_; }

private:
  node_type *root_;
};

template <typename P> class node {
  using key_type = fix_sized_key<P::ikey_size>;
  using value_type = typename P::value_type;
  using threadinfo = typename P::threadinfo_type;
  using node_type = node<P>;

public:
  key_type key_;
  value_type *pValue_;
  node<P> *pLeft_;
  node<P> *pRight_;

  node_type *child(int direction) {
    if (direction > 0)
      return pRight_;
    else if (direction < 0)
      return pLeft_;
    assert(false);
  }

  node(Str k)
      : key_(key_type(k)), pValue_(nullptr), pLeft_(nullptr), pRight_(nullptr) {
  }

  static node_type *make(Str key, threadinfo &ti) {
    void *data = ti.pool_allocate(sizeof(node_type), memtag_masstree_internode);
    return new (data) node_type(key);
  }
};

template <typename P> void binary_tree<P>::initialize(threadinfo &ti) {
  root_ = node_type::make("", ti);
}

template <typename P> class cursor {
public:
  using node_type = node<P>;
  using value_type = typename P::value_type;
  using key_type = fix_sized_key<P::ikey_size>;
  using threadinfo = typename P::threadinfo_type;

  cursor(const binary_tree<P> &tree, Str key)
      : node_(nullptr), k_(key), pv_(nullptr), root_(tree.root()) {}

  cursor(const binary_tree<P> &tree, const char *key)
      : node_(nullptr), k_(key_type(key)), pv_(nullptr), root_(tree.root()) {}

  bool find() {
    node_ = const_cast<node_type *>(root_);
    while (node_) {
      int cmp = k_.compare(node_->key_);
      if (cmp == 0) {
        pv_ = node_->pValue_;
        return true;
      } else {
        parent_ = node_;
        node_ = node_->child(cmp);
      }
    }
    return false;
  }

private:
  node_type *parent_;
  node_type *node_;
  key_type k_;
  value_type *pv_;
  const node_type *root_;

  friend class query;
};

class query {
public:
  template <typename T> typename T::value_type *get(T &tree, Str key) {
    typename T::cursor_type c(tree, key);
    if (c.find()) {
      return c.pv_;
    } else {
      return nullptr;
    }
  }

  template <typename T, typename V>
  void put(T &tree, Str key, V value, threadinfo &ti) {
    using value_type = typename T::value_type;
    using node_type = typename T::node_type;
    typename T::cursor_type c(tree, key);
    value_type *newValue = (value_type *)ti.allocate(sizeof(value_type), memtag_value);
    new (newValue) value_type(value);
    if (c.find()) {
      cmpxchg<value_type *>(&c.node_->pValue_, c.pv_, newValue);
    } else {
      node_type *node = node_type::make(key, ti);
      node->pValue_ = newValue;
      int dir = c.k_.compare(c.parent_->key_);
      cmpxchg<node_type *>(dir > 0 ? &c.parent_->pRight_ : &c.parent_->pLeft_, nullptr, node);
      c.node_ = node;
    }
  }
};
} // namespace binarytree
#endif