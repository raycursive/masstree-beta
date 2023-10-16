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
#include "blib/AVLTree.hpp"

namespace avltree {
using lcdf::Str;
using lcdf::String;
using std::max;
using std::stack;
typedef int64_t height_t;

class query {
public:
    template<typename T>
    typename T::value_type *get(T &tree, Str key) {
        typename T::cursor_type c(tree, key);
        if (c.find()) {
            return c.pv_;
        } else {
            return nullptr;
        }
    }

    template<typename T>
    void put(T &tree, Str key, typename T::value_type value, threadinfo &ti) {
        using value_type = typename T::value_type;
        using node_type = typename T::node_type;

        // allocate new value ptr
        value_type *newValue = (value_type *) ti.pool_allocate(sizeof(value_type), memtag_value);
        new(newValue) value_type(value);
        node_type *newNode = nullptr;

        retry:
        typename T::cursor_type c(tree, key);
        auto stk = c.find_with_stack();
        bool found = !stk.empty() && stk.top().cmp == 0;

        if (found) {
            // perform update
            release_fence();
            while (true) {
                if (bool_cmpxchg<value_type *>(&c.node_->pValue_, c.pv_, newValue)) {
                    break;
                }
                relax_fence();
            }
            ti.pool_deallocate(c.pv_, sizeof(value_type), memtag_value);
        } else {
            // perform insert
            if (!newNode) {
                newNode = node_type::make(key, ti);
                newNode->pValue_ = newValue;
            }
            release_fence();
            int dir = c.k_.compare(c.parent_->key_);
            if (bool_cmpxchg<node_type *>(dir > 0 ? &c.parent_->pRight_
                                                  : &c.parent_->pLeft_,
                                          nullptr, newNode)) {
                c.node_ = newNode;

                //  Balance
                auto prev = newNode;
                while (!stk.empty()) {
                    auto el = stk.top();
                    stk.pop();
                    if (el.cmp > 0) {
                        // Right
                        el.p_node->pRight_ = prev;
                    } else {
                        el.p_node->pLeft_ = prev;
                    }

                    el.p_node->recompute_height();
                    prev = AVLHelpers<node_type>::balance_height(el.p_node);
                }

                tree.set_root(prev);
            } else {
                goto retry;
            }
        }
    }
};

using default_table = avl_tree <tree_params<16>>;
// static_assert(sizeof(default_table::node_type) == 40,
//               "binary tree node size is not 40 bytes");
} // namespace avltree
#endif