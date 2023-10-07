#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/utsname.h>
#include <limits.h>
#if HAVE_NUMA_H
#include <numa.h>
#endif
#if HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#if HAVE_EXECINFO_H
#include <execinfo.h>
#endif
#if __linux__
#include <asm-generic/mman.h>
#endif
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <signal.h>
#include <errno.h>
#ifdef __linux__
#include <malloc.h>
#endif
#include "nodeversion.hh"
#include "kvstats.hh"
#include "query_masstree.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "timestamp.hh"
#include "json.hh"
#include "kvtest.hh"
#include "kvrandom.hh"
#include "kvrow.hh"
#include "kvio.hh"
#include "clp.h"
#include <algorithm>
#include <numeric>

template <typename T>
class kvtest_client {
public:
    using table_type = T;

    int nthreads() const;
    int id() const;
    void set_table(T* table, threadinfo *ti);
    void reset(const String &test, int trial);

    bool timeout(int which) const;
    uint64_t limit() const;
    bool has_param(const String& name) const;
    Json param(const String& name, Json default_value = Json()) const;

    int ncores() const;
    double now() const;
    int ruscale_partsz() const;
    int ruscale_init_part_no() const;
    long nseqkeys() const;

    void get(long ikey);
    bool get_sync(Str key);
    bool get_sync(Str key, Str& value);
    bool get_sync(long ikey);
    bool get_sync_key16(long ikey);
    void get_check(Str key, Str expected);
    void get_check(const char *key, const char *expected);
    void get_check(long ikey, long iexpected);
    void get_check(Str key, long iexpected);
    void get_check_key8(long ikey, long iexpected);
    void get_col_check(Str key, int col, Str value);
    void get_col_check(long ikey, int col, long ivalue);
    void get_col_check_key10(long ikey, int col, long ivalue);
    void get_check_absent(Str key);
    //void many_get_check(int nk, long ikey[], long iexpected[]);

    void scan_sync(Str firstkey, int n,
                   std::vector<Str>& keys, std::vector<Str>& values);
    void rscan_sync(Str firstkey, int n,
                    std::vector<Str>& keys, std::vector<Str>& values);
    void scan_versions_sync(Str firstkey, int n,
                            std::vector<Str>& keys, std::vector<Str>& values);
    const std::vector<uint64_t>& scan_versions() const;

    void put(Str key, Str value);
    void put(const char *key, const char *value);
    void put(long ikey, long ivalue);
    void put(Str key, long ivalue);
    void put_key8(long ikey, long ivalue);
    void put_key16(long ikey, long ivalue);
    void put_col(Str key, int col, Str value);
    void put_col(long ikey, int col, long ivalue);
    void put_col_key10(long ikey, int col, long ivalue);
    void insert_check(Str key, Str value);

    void remove(Str key);
    void remove(long ikey);
    void remove_key8(long ikey);
    void remove_key16(long ikey);
    bool remove_sync(Str key);
    bool remove_sync(long ikey);
    void remove_check(Str key);

    void print();
    void puts_done();
    void wait_all();
    void rcu_quiesce();
    String make_message(lcdf::StringAccum &sa) const;
    void notice(const char *fmt, ...);
    void fail(const char *fmt, ...);
    const Json& report(const Json& x);
    void finish();

private:
    void output_scan(const Json& req, std::vector<Str>& keys, std::vector<Str>& values) const;
};