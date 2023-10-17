#ifndef AVLTREE_HH
#define AVLTREE_HH

#include "circular_int.hh"
#include "compiler.hh"
#include "fixsizedkey.hh"
#include "kvthread.hh"
#include "mtcounters.hh"
#include "str.hh"
#include "string.hh"
#include <stdio.h>
#include <string>
#include <stack>
#include <algorithm>
#include <utility>
#include "blib/AVLOld.hpp"

#define THREAD_COUNT 16

namespace avltree {
using lcdf::Str;
using lcdf::String;
using std::max;
using std::stack;
typedef int64_t height_t;

using lcdf::Str;
using lcdf::String;
using std::max;
using std::stack;
typedef int64_t height_t;

template<size_t KeySize = 16>
struct tree_params {
    static constexpr int ikey_size = KeySize;
    static constexpr bool enable_int_cmp = true;
    using value_type = std::string;
    using threadinfo_type = ::threadinfo;
};


template<typename P>
class node;

template<typename P>
class cursor;

template<typename P>
class avl_tree {
public:
    using parameter_type = P;
    using node_type = node<P>;
    using value_type = typename P::value_type;
    using key_type = fix_sized_key<P::ikey_size, P::enable_int_cmp>;
    using threadinfo = typename P::threadinfo_type;
    using cursor_type = cursor<P>;

    AVLTree<P, THREAD_COUNT> tree_obj;

    inline avl_tree() {}

    void initialize(threadinfo &ti);

    void destroy(threadinfo &ti);

    inline node_type *root() const { return root_; }

    inline void set_root(node_type *rt) { root_ = rt; }

    static const char *name() { return "avltree"; }

    void print(FILE *f) const {
        print(f, root_);
    }

private:
    node_type *root_;

    void print(FILE *f, node_type *node) const {
        
    }

    void print(FILE *f, const std::string &prefix, const node_type *node, bool isLeft) const {
        
    }
};

template<typename P>
void avl_tree<P>::initialize(threadinfo &ti) {
    // root_ = node_type::make("", ti);
}

class query {
public:
    template<typename T>
    typename T::value_type *get(T &tree, Str key) {
        //  using value_type = T::value_type;
         string *value = nullptr;
        //  string k(key.data());
        int k = std::stoi(key.data());

         if(tree.tree_obj.get(k, value)) {
            return value;
         } else {
            return nullptr;
         }
    }

    template<typename T>
    void put(T &tree, Str key, typename T::value_type value, threadinfo &ti) {
        value_type *val_p = (value_type *) ti.pool_allocate(sizeof(value_type), memtag_value);
        new(val_p) value_type(value);
        int k = std::stoi(key.data());
        // string k(key.data());
        tree.tree_obj.add(k, val_p);
    }
};

using default_table = avl_tree <tree_params<16>>;
// static_assert(sizeof(default_table::node_type) == 40,
//               "binary tree node size is not 40 bytes");
} // namespace avltree
#endif