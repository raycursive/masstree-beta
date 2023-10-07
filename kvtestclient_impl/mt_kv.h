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
#include "mttest.h"

static std::vector<int> cores;
volatile bool timeout[2] = {false, false};
double duration[2] = {10, 0};
int kvtest_first_seed = 31949;
uint64_t test_limit = ~uint64_t(0);
static Json test_param;

bool quiet = false;
bool print_table = false;
static const char *gid = NULL;

// all default to the number of cores
static int udpthreads = 0;
static int tcpthreads = 0;

static bool tree_stats = false;
static bool json_stats = false;
static String gnuplot_yrange;
static bool pinthreads = false;
static nodeversion32 global_epoch_lock(false);
volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;
kvepoch_t global_log_epoch = 0;
static int port = 2117;
static int rscale_ncores = 0;

#if MEMSTATS && HAVE_NUMA_H && HAVE_LIBNUMA
struct mttest_numainfo {
    long long free;
    long long size;
};
std::vector<mttest_numainfo> numa;
#endif

volatile bool recovering = false; // so don't add log entries, and free old value immediately
kvtimestamp_t initial_timestamp;

static const char *threadcounter_names[(int) tc_max];

/* running local tests */
void test_timeout(int) {
    size_t n;
    for (n = 0; n < arraysize(timeout) && timeout[n]; ++n)
        /* do nothing */;
    if (n < arraysize(timeout)) {
        timeout[n] = true;
        if (n + 1 < arraysize(timeout) && duration[n + 1])
            xalarm(duration[n + 1]);
    }
}

void set_global_epoch(mrcu_epoch_type e) {
    global_epoch_lock.lock();
    if (mrcu_signed_epoch_type(e - globalepoch) > 0) {
        globalepoch = e;
        active_epoch = threadinfo::min_active_epoch();
    }
    global_epoch_lock.unlock();
}

template <typename T>
class kvtest_client_mt: public kvtest_client<T> {
public:
    using table_type = T;
    int ncores_;
    uint64_t limit_;
    kvout *kvo_;
    kvtest_client_mt()
            : limit_(test_limit), ncores_(udpthreads), kvo_() {
    }
    ~kvtest_client_mt() {
        if (kvo_)
            free_kvout(kvo_);
    }

    int nthreads() const {
        return udpthreads;
    }
    int id() const {
        return ti_->index();
    }
    void set_table(T* table, threadinfo *ti) {
        table_ = table;
        ti_ = ti;
    }
    void reset(const String &test, int trial) {
        report_ = Json().set("table", T().name())
                .set("test", test).set("trial", trial)
                .set("thread", ti_->index());
    }

    bool timeout(int which) const {
        return ::timeout[which];
    }
    uint64_t limit() const {
        return limit_;
    }
    bool has_param(const String& name) const {
        return test_param.count(name);
    }
    Json param(const String& name, Json default_value = Json()) const {
        return test_param.count(name) ? test_param.at(name) : default_value;
    }

    int ncores() const {
        return ncores_;
    }
    double now() const {
        return ::now();
    }
    int ruscale_partsz() const {
        return (140 * 1000000) / 16;
    }
    int ruscale_init_part_no() const {
        return ti_->index();
    }
    long nseqkeys() const {
        return 16 * ruscale_partsz();
    }

    void get(long ikey);
    bool get_sync(Str key);
    bool get_sync(Str key, Str& value);
    bool get_sync(long ikey) {
        quick_istr key(ikey);
        return get_sync(key.string());
    }
    bool get_sync_key16(long ikey) {
        quick_istr key(ikey, 16);
        return get_sync(key.string());
    }
    void get_check(Str key, Str expected);
    void get_check(const char *key, const char *expected) {
        get_check(Str(key), Str(expected));
    }
    void get_check(long ikey, long iexpected) {
        quick_istr key(ikey), expected(iexpected);
        get_check(key.string(), expected.string());
    }
    void get_check(Str key, long iexpected) {
        quick_istr expected(iexpected);
        get_check(key, expected.string());
    }
    void get_check_key8(long ikey, long iexpected) {
        quick_istr key(ikey, 8), expected(iexpected);
        get_check(key.string(), expected.string());
    }
    void get_col_check(Str key, int col, Str value);
    void get_col_check(long ikey, int col, long ivalue) {
        quick_istr key(ikey), value(ivalue);
        get_col_check(key.string(), col, value.string());
    }
    void get_col_check_key10(long ikey, int col, long ivalue) {
        quick_istr key(ikey, 10), value(ivalue);
        get_col_check(key.string(), col, value.string());
    }
    void get_check_absent(Str key);
    //void many_get_check(int nk, long ikey[], long iexpected[]);

    void scan_sync(Str firstkey, int n,
                   std::vector<Str>& keys, std::vector<Str>& values);
    void rscan_sync(Str firstkey, int n,
                    std::vector<Str>& keys, std::vector<Str>& values);
    void scan_versions_sync(Str firstkey, int n,
                            std::vector<Str>& keys, std::vector<Str>& values);
    const std::vector<uint64_t>& scan_versions() const {
        return scan_versions_;
    }

    void put(Str key, Str value);
    void put(const char *key, const char *value) {
        put(Str(key), Str(value));
    }
    void put(long ikey, long ivalue) {
        quick_istr key(ikey), value(ivalue);
        put(key.string(), value.string());
    }
    void put(Str key, long ivalue) {
        quick_istr value(ivalue);
        put(key, value.string());
    }
    void put_key8(long ikey, long ivalue) {
        quick_istr key(ikey, 8), value(ivalue);
        put(key.string(), value.string());
    }
    void put_key16(long ikey, long ivalue) {
        quick_istr key(ikey, 16), value(ivalue);
        put(key.string(), value.string());
    }
    void put_col(Str key, int col, Str value);
    void put_col(long ikey, int col, long ivalue) {
        quick_istr key(ikey), value(ivalue);
        put_col(key.string(), col, value.string());
    }
    void put_col_key10(long ikey, int col, long ivalue) {
        quick_istr key(ikey, 10), value(ivalue);
        put_col(key.string(), col, value.string());
    }
    void insert_check(Str key, Str value);

    void remove(Str key);
    void remove(long ikey) {
        quick_istr key(ikey);
        remove(key.string());
    }
    void remove_key8(long ikey) {
        quick_istr key(ikey, 8);
        remove(key.string());
    }
    void remove_key16(long ikey) {
        quick_istr key(ikey, 16);
        remove(key.string());
    }
    bool remove_sync(Str key);
    bool remove_sync(long ikey) {
        quick_istr key(ikey);
        return remove_sync(key.string());
    }
    void remove_check(Str key);

    void print() {
        table_->print(stderr);
    }
    void puts_done() {
    }
    void wait_all() {
    }
    void rcu_quiesce() {
        mrcu_epoch_type e = timestamp() >> 16;
        if (e != globalepoch)
            set_global_epoch(e);
        ti_->rcu_quiesce();
    }
    String make_message(lcdf::StringAccum &sa) const;
    void notice(const char *fmt, ...);
    void fail(const char *fmt, ...);
    const Json& report(const Json& x) {
        return report_.merge(x);
    }
    void finish() {
        Json counters;
        for (int i = 0; i < tc_max; ++i) {
            if (uint64_t c = ti_->counter(threadcounter(i)))
                counters.set(threadcounter_names[i], c);
        }
        if (counters) {
            report_.set("counters", counters);
        }
        if (!quiet) {
            fprintf(stderr, "%d: %s\n", ti_->index(), report_.unparse().c_str());
        }
    }

    T *table_;
    threadinfo *ti_;
    query<row_type> q_[1];
    kvrandom_lcg_nr rand;
    Json report_;
    Json req_;
    std::vector<uint64_t> scan_versions_;

private:
    void output_scan(const Json& req, std::vector<Str>& keys, std::vector<Str>& values) const;
};

static volatile int kvtest_printing;

template <typename T> inline void kvtest_print(const T &table, FILE* f, threadinfo *ti) {
    // only print out the tree from the first failure
    while (!bool_cmpxchg((int *) &kvtest_printing, 0, ti->index() + 1)) {
    }
    table.print(f);
}

template <typename T> inline void kvtest_json_stats(T& table, Json& j, threadinfo& ti) {
    table.json_stats(j, ti);
}

template <typename T>
void kvtest_client_mt<T>::get(long ikey) {
    quick_istr key(ikey);
    Str val;
    (void) q_[0].run_get1(table_->table(), key.string(), 0, val, *ti_);
}

template <typename T>
bool kvtest_client_mt<T>::get_sync(Str key) {
    Str val;
    return q_[0].run_get1(table_->table(), key, 0, val, *ti_);
}

template <typename T>
bool kvtest_client_mt<T>::get_sync(Str key, Str& value) {
    return q_[0].run_get1(table_->table(), key, 0, value, *ti_);
}

template <typename T>
void kvtest_client_mt<T>::get_check(Str key, Str expected) {
    Str val;
    if (unlikely(!q_[0].run_get1(table_->table(), key, 0, val, *ti_))) {
        fail("get(%s) failed (expected %s)\n", String(key).printable().c_str(),
             String(expected).printable().c_str());
    } else if (unlikely(expected != val)) {
        fail("get(%s) returned unexpected value %s (expected %s)\n",
             String(key).printable().c_str(),
             String(val).substr(0, 40).printable().c_str(),
             String(expected).substr(0, 40).printable().c_str());
    }
}

template <typename T>
void kvtest_client_mt<T>::get_col_check(Str key, int col,
                                     Str expected) {
    Str val;
    if (unlikely(!q_[0].run_get1(table_->table(), key, col, val, *ti_))) {
        fail("get.%d(%.*s) failed (expected %.*s)\n",
             col, key.len, key.s, expected.len, expected.s);
    } else if (unlikely(expected != val)) {
        fail("get.%d(%.*s) returned unexpected value %.*s (expected %.*s)\n",
             col, key.len, key.s, std::min(val.len, 40), val.s,
             expected.len, expected.s);
    }
}

template <typename T>
void kvtest_client_mt<T>::get_check_absent(Str key) {
    Str val;
    if (unlikely(q_[0].run_get1(table_->table(), key, 0, val, *ti_))) {
        fail("get(%s) failed (expected absent key)\n", String(key).printable().c_str());
    }
}

/*template <typename T>
void kvtest_client_mt<T>::many_get_check(int nk, long ikey[], long iexpected[]) {
    std::vector<quick_istr> ka(2*nk, quick_istr());
    for(int i = 0; i < nk; i++){
      ka[i].set(ikey[i]);
      ka[i+nk].set(iexpected[i]);
      q_[i].begin_get1(ka[i].string());
    }
    table_->many_get(q_, nk, *ti_);
    for(int i = 0; i < nk; i++){
      Str val = q_[i].get1_value();
      if (ka[i+nk] != val){
        printf("get(%ld) returned unexpected value %.*s (expected %ld)\n",
             ikey[i], std::min(val.len, 40), val.s, iexpected[i]);
        exit(1);
      }
    }
}*/

template <typename T>
void kvtest_client_mt<T>::scan_sync(Str firstkey, int n,
                                 std::vector<Str>& keys,
                                 std::vector<Str>& values) {
    req_ = Json::array(0, 0, firstkey, n);
    q_[0].run_scan(table_->table(), req_, *ti_);
    output_scan(req_, keys, values);
}

template <typename T>
void kvtest_client_mt<T>::rscan_sync(Str firstkey, int n,
                                  std::vector<Str>& keys,
                                  std::vector<Str>& values) {
    req_ = Json::array(0, 0, firstkey, n);
    q_[0].run_rscan(table_->table(), req_, *ti_);
    output_scan(req_, keys, values);
}

template <typename T>
void kvtest_client_mt<T>::scan_versions_sync(Str firstkey, int n,
                                          std::vector<Str>& keys,
                                          std::vector<Str>& values) {
    req_ = Json::array(0, 0, firstkey, n);
    scan_versions_.clear();
    q_[0].run_scan_versions(table_->table(), req_, scan_versions_, *ti_);
    output_scan(req_, keys, values);
}

template <typename T>
void kvtest_client_mt<T>::output_scan(const Json& req, std::vector<Str>& keys,
                                   std::vector<Str>& values) const {
    keys.clear();
    values.clear();
    for (int i = 2; i != req.size(); i += 2) {
        keys.push_back(req[i].as_s());
        values.push_back(req[i + 1].as_s());
    }
}

template <typename T>
void kvtest_client_mt<T>::put(Str key, Str value) {
    q_[0].run_replace(table_->table(), key, value, *ti_);
}

template <typename T>
void kvtest_client_mt<T>::insert_check(Str key, Str value) {
    if (unlikely(q_[0].run_replace(table_->table(), key, value, *ti_) != Inserted)) {
        fail("insert(%s) did not insert\n", String(key).printable().c_str());
    }
}

template <typename T>
void kvtest_client_mt<T>::put_col(Str key, int col, Str value) {
#if !MASSTREE_ROW_TYPE_STR
    if (!kvo_) {
        kvo_ = new_kvout(-1, 2048);
    }
    Json x[2] = {Json(col), Json(String::make_stable(value))};
    q_[0].run_put(table_->table(), key, &x[0], &x[2], *ti_);
#else
    (void) key, (void) col, (void) value;
    assert(0);
#endif
}

template <typename T> inline bool kvtest_remove(kvtest_client_mt<T> &client, Str key) {
    return client.q_[0].run_remove(client.table_->table(), key, *client.ti_);
}

template <typename T>
void kvtest_client_mt<T>::remove(Str key) {
    (void) kvtest_remove(*this, key);
}

template <typename T>
bool kvtest_client_mt<T>::remove_sync(Str key) {
    return kvtest_remove(*this, key);
}

template <typename T>
void kvtest_client_mt<T>::remove_check(Str key) {
    if (unlikely(!kvtest_remove(*this, key))) {
        fail("remove(%s) did not remove\n", String(key).printable().c_str());
    }
}

template <typename T>
String kvtest_client_mt<T>::make_message(lcdf::StringAccum &sa) const {
    const char *begin = sa.begin();
    while (begin != sa.end() && isspace((unsigned char) *begin))
        ++begin;
    String s = String(begin, sa.end());
    if (!s.empty() && s.back() != '\n')
        s += '\n';
    return s;
}

template <typename T>
void kvtest_client_mt<T>::notice(const char *fmt, ...) {
    va_list val;
    va_start(val, fmt);
    String m = make_message(lcdf::StringAccum().vsnprintf(500, fmt, val));
    va_end(val);
    if (m && !quiet)
        fprintf(stderr, "%d: %s", ti_->index(), m.c_str());
}

template <typename T>
void kvtest_client_mt<T>::fail(const char *fmt, ...) {
    static nodeversion32 failing_lock(false);
    static nodeversion32 fail_message_lock(false);
    static String fail_message;

    va_list val;
    va_start(val, fmt);
    String m = make_message(lcdf::StringAccum().vsnprintf(500, fmt, val));
    va_end(val);
    if (!m)
        m = "unknown failure";

    fail_message_lock.lock();
    if (fail_message != m) {
        fail_message = m;
        fprintf(stderr, "%d: %s", ti_->index(), m.c_str());
    }
    fail_message_lock.unlock();

    failing_lock.lock();
    fprintf(stdout, "%d: %s", ti_->index(), m.c_str());
    kvtest_print(*table_, stdout, ti_);

    always_assert(0);
}