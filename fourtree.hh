#ifndef FOURTREE_HH
#define FOURTREE_HH
#include "circular_int.hh"
#include "compiler.hh"
#include "fixsizedkey.hh"
#include "kvrow.hh"
#include "kvthread.hh"
#include "mtcounters.hh"
#include "nodeversion.hh"
#include "str.hh"
#include "string.hh"
#include <mutex>
#include <stdio.h>
#include <string>

namespace fourtree {
using lcdf::Str;
using lcdf::String;

template <size_t KeySize = 16> struct tree_params {
  static constexpr int ikey_size = KeySize;
  static constexpr int fanout = 4;
  using value_type = row_type;
  using threadinfo_type = ::threadinfo;
  using nodeversion_value_type = uint32_t;
};


template <typename P> class node;
template <typename P> class cursor;

template <typename P> class four_tree {
public:
  using parameter_type = P;
  using node_type = node<P>;
  using value_type = typename P::value_type;
  using key_type = fix_sized_key<P::ikey_size>;
  using threadinfo = typename P::threadinfo_type;
  using cursor_type = cursor<P>;

  inline four_tree() {}

  void initialize(threadinfo &ti);
  void destroy(threadinfo &ti);

  inline node_type *root() const { return root_; }

  static const char *name() { return "fourtree"; }

  void print(FILE *f) const { print(f, root_); }

private:
  node_type *root_;

  void print(FILE *f, node_type *node) const { print(f, "", node, false, 0); }

  void print(FILE *f, const std::string &prefix, const node_type *node,
             bool isLeft, int layer) const {
    if (node != nullptr) {
      fprintf(f, "%s", prefix.c_str());

      fprintf(f, "%s C(%d)= ", (isLeft ? "├──" : "└──"), layer);

      // print the value of the node
      for (int i = 0; i < P::fanout - 1; i++) {
        if (node->values_[i] == nullptr) {
          fprintf(f, "%d:(%s:%s); ", i, "null", "null");
        } else {
          key_type k(node->keys0_[i], node->keys1_[i]);
          fprintf(f, "%d:(%s:%s); ", i, k.unparse_printable().c_str(),
                  node->values_[i]->col(0).data());
        }
      }
      fprintf(f, "\n");

      // print next level
      for (int i = 0; i < P::fanout; i++) {
        print(f, prefix + (isLeft ? "│   " : "    "), node->children_[i],
              i != P::fanout - 1, i);
      }
    }
  }
};

template <typename P>
class alignas(CACHE_LINE_SIZE) node
    : public nodeversion<nodeversion_parameters<typename P::nodeversion_value_type>> {
public:
  using key_type = fix_sized_key<P::ikey_size>;
  using value_type = typename P::value_type;
  using threadinfo = typename P::threadinfo_type;
  using node_type = node<P>;
  using nodeversion_type = nodeversion<nodeversion_parameters<typename P::nodeversion_value_type>>;

  // first cacheline contains first 8 bytes of keys and 4 children
  uint64_t keys0_[P::fanout - 1] = {};
  node<P> *children_[P::fanout] = {};
  uint64_t keys1_[P::fanout - 1] = {};
  value_type *values_[P::fanout - 1] = {};

  node() {}

  static node_type *make(threadinfo &ti) {
    node_type *data = (node_type *)ti.pool_allocate(sizeof(node_type),
                                                    memtag_masstree_internode);
    new (data) node_type();
    return data;
  }

  int compare_with(const key_type &k, size_t index) const {
    int cmp = ::compare(k.ikey_u.ikey[0], keys0_[index]);
    if (cmp == 0) {
      cmp = ::compare(k.ikey_u.ikey[1], keys1_[index]);
    }
    return cmp;
  }

  void assign(const key_type &k, Str v, size_t index, threadinfo &ti) {
    value_type *pNewValue = value_type::create1(v, 0, ti);
    // value_type *pNewValue =
    //     (value_type *)ti.pool_allocate(sizeof(value_type), memtag_value);
    // new (pNewValue) value_type(v);
    keys0_[index] = k.ikey_u.ikey[0];
    keys1_[index] = k.ikey_u.ikey[1];
    value_type *oldValue = values_[index];
    values_[index] = pNewValue;
    if   (oldValue != nullptr) {
      // ti.pool_deallocate(oldValue, sizeof(value_type), memtag_value);
      oldValue->deallocate_rcu(ti);
    }
  }
  node_type *route(const key_type &k, size_t &index) const {
    nodeversion_type v_;
    return this->route(k, index, v_);
  }

  node_type *route(const key_type &k, size_t &index, nodeversion_type &v) const {
    retry:
    v = this->stable();
    for (int i = 0; i < P::fanout - 1; i++) {
      // no value in current index
      if (values_[i] == nullptr) {
        if (unlikely(this->has_changed(v))) {
          goto retry;
        }
        index = i;
        return const_cast<node_type *>(this);
      }
      int cmp = compare_with(k, i);
      if (cmp == 0) {
        // found
        if (unlikely(this->has_changed(v))) {
          goto retry;
        }
        index = i;
        return const_cast<node_type *>(this);
      }
      if (cmp < 0) {
        if (unlikely(this->has_changed(v))) {
          goto retry;
        }
        index = i;
        return children_[i];
      }
      // proceed to next index
    }
    if (unlikely(this->has_changed(v))) {
      goto retry;
    }
    index = P::fanout - 1;
    return children_[P::fanout - 1];
  }
};

template <typename P> void four_tree<P>::initialize(threadinfo &ti) {
  root_ = node_type::make(ti);
}

template <typename P> class cursor {
public:
  using node_type = node<P>;
  using value_type = typename P::value_type;
  using key_type = fix_sized_key<P::ikey_size>;
  using threadinfo = typename P::threadinfo_type;

  cursor(const four_tree<P> &tree, Str key)
      : parent_(nullptr), node_(nullptr), index_(0), k_(key), pv_(nullptr),
        root_(tree.root()) {}

  cursor(const four_tree<P> &tree, const char *key)
      : parent_(nullptr), node_(nullptr), index_(0), k_(key_type(key)),
        pv_(nullptr), root_(tree.root()) {}

  void find_unlocked() {
    node_ = const_cast<node_type *>(root_);
    index_ = 0;

    while (node_) {
      node_type *next = node_->route(k_, index_);
      if (next == node_) {
        pv_ = node_->values_[index_];
        return;
      } else {
        parent_ = node_;
        node_ = next;
      }
    }
    // reach an empty node
  }

  void find_locked() {
    retry:
    if (!parent_) {
      node_ = const_cast<node_type *>(root_);
    } else {
      node_ = parent_;
    }
    index_ = 0;
    typename node_type::nodeversion_type v_;
    while (node_) {
      index_ = 0;
      node_type *next = node_->route(k_, index_, v_);
      if (next == node_) {
        // found the existed node
        if (unlikely(node_->has_changed(v_))) {
          goto retry;
        }
        if (!node_->try_lock()) {
          goto retry;
        }
        pv_ = node_->values_[index_];
        return;
      } else {
        parent_ = node_;
        node_ = next;
      }
    }
    // reach an empty node
    if (unlikely(parent_->has_changed(v_))) {
      goto retry;
    }
    if (!parent_->try_lock()) {
      goto retry;
    }
  }

private:
  node_type *parent_;
  node_type *node_;
  size_t index_;
  key_type k_;
  value_type *pv_;
  const node_type *root_;

  friend class query;
};

class query {
public:
  template <typename T> typename T::value_type *get(T &tree, Str key) {
    typename T::cursor_type c(tree, key);
    c.find_unlocked();
    return c.pv_;
  }

  template <typename T>
  void put(T &tree, Str key, Str value, threadinfo &ti) {
    using node_type = typename T::node_type;
    using key_type = typename T::key_type;

    typename T::cursor_type c(tree, key);
    c.find_locked();
    if (!c.node_) {
      c.parent_->mark_insert();
      // create new node and save to the first
      node_type *node = node_type::make(ti);
      node->assign(key_type(key), value, 0, ti);
      c.parent_->children_[c.index_] = node;
      c.parent_->unlock();
    } else {
      c.node_->mark_insert();
      c.node_->assign(key_type(key), value, c.index_, ti);
      c.node_->unlock();
    }
  }
};

using default_table = four_tree<tree_params<16>>;
} // namespace fourtree
#endif