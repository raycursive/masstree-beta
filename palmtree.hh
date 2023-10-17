#ifndef PALMTREE_HH
#define PALMTREE_HH

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
#include "palmtree_orig.hh"

namespace palmtree {
using lcdf::Str;
using lcdf::String;
using std::max;
using std::string;
using std::stoi;
typedef int64_t height_t;

template<size_t KeySize = 16>
struct tree_params {
    static constexpr int ikey_size = KeySize;
    static constexpr bool enable_int_cmp = true;
    using value_type = Str;
    using threadinfo_type = ::threadinfo;
};


template<typename P>
class node;

template<typename P>
class cursor;

template<typename P>
class palm_tree {
public:
    using parameter_type = P;
    using node_type = node<P>;
    using value_type = typename P::value_type;
    // using value_type = int64_t;
    using key_type = int64_t;//fix_sized_key<P::ikey_size, P::enable_int_cmp>;
    using threadinfo = typename P::threadinfo_type;
    using cursor_type = cursor<P>;

    PalmTree<key_type, value_type> *ptree;

    inline palm_tree() {}

    void initialize(threadinfo &ti) {
        // tree = new PalmTree(1, 1);
        PalmTree<key_type, value_type> newTree(std::numeric_limits<int>::min(), 1);
        ptree = &newTree;
    }

    void destroy(threadinfo &ti);

    static const char *name() { return "palmtree"; }

    void print(FILE *f) const {
        // print(f, root_);
    }

private:
    void print(FILE *f, node_type *node) const {
       
    }

    void print(FILE *f, const std::string &prefix, const node_type *node, bool isLeft) const {
        
    }
};

class query {
public:
    template<typename T>
    Str *get(T &tree, Str key) {
        Str res = "NADA_YADA";
        int64_t k = stoi(key.data());

        if (tree.ptree->find(k, res)) {
            return &res;
        } else {
            return nullptr;
        }
    }

    template<typename T>
    void put(T &tree, Str key, typename T::value_type value, threadinfo &ti) {
        using value_type = typename T::value_type;
        using node_type = typename T::node_type;
        int64_t k = stoi(key.data());

        tree.ptree->insert(k, value);
    }
};

using default_table = palm_tree <tree_params<16>>;
// static_assert(sizeof(default_table::node_type) == 40,
//               "binary tree node size is not 40 bytes");
} // namespace palmtree
#endif