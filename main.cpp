#include <iostream>
#include "ConcurrentPartiallyExternalTree.h"
#include "Old.h"
#include "ConcurrentAVL.h"
#include <set>
#include <atomic>
#include <thread>
#include <random>
#include <mutex>
#include <future>
#include <cds/init.h>
#include <cds/threading/model.h>
#include <cds/container/bronson_avltree_map_rcu.h>
#include <cds/container/ellen_bintree_set_rcu.h>
#include <cds/urcu/general_buffered.h>
#include <ppl.h>
#include <concurrent_unordered_map.h>
#include <concurrent_priority_queue.h>
#include <shared_mutex>
#include <chrono>
#include "ConcurrentAVL_LO_old.h"
#include "Concurrent_AVL_LO.h"
using namespace std::chrono_literals;
typedef cds::urcu::gc< cds::urcu::general_buffered<> >  rcu_gpb;


template<typename Container>
int randCommands(Container& container, int seed, int range,
                 long long milliseconds,int p_insert, int p_erase){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
//    int c = 0;
    int fake = 0;
//    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<milliseconds;++i){
        int r = rnd()%100;
        if(r <p_insert){
//            container.insert(i*seed);
            container.insert(rnd()%range);
        }else if(r < p_insert+p_erase){
            container.erase(rnd()%range);
        }else{
            if(container.contains(rnd()%range)){
                fake++;
            };
        }
//        c++;
//        auto end=std::chrono::high_resolution_clock::now();
//        if((end - start).count() > 1'000'000 * milliseconds) break;
    }
    cds::threading::Manager::detachThread();
    return fake%2;
}

int randCommands(std::set<int>& set, int seed, std::shared_mutex& mtx,int range,
                 long long milliseconds, int p_insert, int p_erase){
    std::minstd_rand rnd(seed);
//    std::lock_guard lock(mtx);
//    int c = 0;
    int fake = 0;
//    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<milliseconds;++i){
        int r = rnd()%100;
        if(r <p_insert){
            std::lock_guard lock(mtx);
            set.insert(rnd()%range);
        }else if(r < p_insert + p_erase){
            std::lock_guard lock(mtx);
            set.erase(rnd()%range);
        }else{
            std::shared_lock lock(mtx);
            if(set.find(rnd()%range)!=set.end()){
                fake++;
            }
        }
//        c++;
//        auto end=std::chrono::high_resolution_clock::now();
//        if((end - start).count() > 1'000'000 * milliseconds) break;
    }
    return fake%2;
}
template<typename Func>
void start_threads(int n_threads,  Func f){

    std::vector<std::thread> threads(n_threads);

    for(int i=0;i<n_threads;++i){
        threads[i] = std::thread(f);
    }
    for(int i=0;i<n_threads;++i){
        threads[i].join();
    }
}

template<typename Func>
long long start_threads(int n_threads,std::minstd_rand& rnd,  Func f){
    int seed = rnd();
    std::minstd_rand local_rnd(seed);
    std::vector<std::thread> threads(n_threads);
    std::vector<int> results(n_threads);
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<n_threads;++i){
        threads[i] = std::thread(f, local_rnd(), std::ref(results[i]));
    }
//    int sum = 0;
    for(int i=0;i<n_threads;++i){
        threads[i].join();
//        sum+=results[i];
    }
    auto end=std::chrono::high_resolution_clock::now();
    return (end - start).count();

//    return sum;
}
template<typename Container>
int stress(Container& container, int seed, int range, int count, int p_insert, int p_erase){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    int fake = 0;
    for(int i=0;i<count;++i){
        int r = rnd()%100;
        if(r < p_insert){
//            container.insert(i*seed);
            container.insert(rnd()%range);
        }else if(r < p_insert + p_erase){
            container.erase(rnd()%range);
        }else{
            if(container.contains(rnd()%range)){
                fake++;
            };
        }
    }
    cds::threading::Manager::detachThread();
    return fake%2;
}
template<typename Container>
void init(Container& container){
    cds::threading::Manager::attachThread();
    randCommands(container, 1,1000000000,2000LL, 100, 0);
    cds::threading::Manager::detachThread();
}
template<typename Func>
void checkCo(Func co_set_test, int seed, long long sh_res, int i,
             ConcurrentPartiallyExternalTree<int>*& co_set, bool needInit) {
    std::minstd_rand rnd(seed);
    cds::Initialize();
    cds::gc::HP gc;
    ConcurrentPartiallyExternalTree<int> l_set;
    co_set = &l_set;
    if(needInit) {
        init(l_set);
        std::cout << "(i) ";
    }
    long long res = start_threads(i, rnd, co_set_test);
    std::cout << "partially "<< (double)sh_res/res << ' ' << res << std::endl;
    cds::Terminate();
}
template<typename Func>
void checkMtx(Func sm_set_test, int i, int &seed, long long &sh_res, std::set<int>*& m_set) {
    std::minstd_rand rnd(seed);
    std::set<int> mutex_set;
    m_set = &mutex_set;
    sh_res = start_threads(i, rnd, sm_set_test);
    std::cout << "shared_mutex  " << 1 << ' ' << sh_res << std::endl;
}

template<typename Func>
void checkLo(Func lo_set_test, ConcurrentAVL_LO<int>*& lo_set, int seed,
             long long sh_res, int i, bool needInit) {
    std::minstd_rand rnd(seed);
    cds::Initialize();
    cds::gc::HP gc;
    ConcurrentAVL_LO<int> l_set;
    lo_set = &l_set;
    cds::threading::Manager::attachThread();
    l_set.insert(-10);
    cds::threading::Manager::detachThread();
    if(needInit) {
        init(l_set);
        std::cout<<"(i) ";
    }
    long long res = start_threads(i, rnd, lo_set_test);
    std::cout << "logic_order " << (double)sh_res/res << ' ' << res << std::endl;
    cds::Terminate();
}

void test_time(int s_threads, int e_threads, int range, long long milliseconds, int p_insert, int p_erase){

    std::set<int>* m_set;
    std::shared_mutex mtx;
    auto sm_set_test = [&](int seed, int& res){
        res = randCommands(*m_set, seed, mtx, range,milliseconds,  p_insert, p_erase);
    };
    ConcurrentAVL_LO<int>* lo_set;
    auto lo_set_test = [&](int seed, int& res){
        res = randCommands(*lo_set, seed,range,milliseconds, p_insert, p_erase);
    };
//    ConcurrentAVL_LO_old<int>* loo_set;
//    auto loo_set_test = [&](int seed, int& res){
//        res = randCommands<ConcurrentAVL_LO_old<int>, 1000, 2000LL>(*loo_set, seed, 50, 50);
//    };
    ConcurrentPartiallyExternalTree<int>* co_set;
    auto co_set_test = [&](int seed, int& res){
        res = randCommands(*co_set, seed,range,milliseconds, p_insert, p_erase);
    };
    cds::container::BronsonAVLTreeMap<rcu_gpb, int, int>* el;

    auto el_set_test = [&](int seed, int& res){
        res = randCommands(*el, seed,range,milliseconds, p_insert, p_erase);
    };

    std::minstd_rand rndm(time(NULL));

    int seed = rndm();

    long long sh_res;
    for(int i=s_threads;i<=e_threads; ++i){
        std::cout << "\nthreads: " << i << std::endl;
        checkMtx(sm_set_test, i, seed, sh_res, m_set);
        checkLo(lo_set_test,lo_set, seed, sh_res, i, true);
        checkLo(lo_set_test,lo_set, seed, sh_res, i, false);
        checkCo(co_set_test, seed, sh_res, i, co_set, true);
        checkCo(co_set_test, seed, sh_res, i, co_set, false);
    }

    cds::Initialize();
    rcu_gpb gpbRCU;
    {
        cds::container::BronsonAVLTreeMap<rcu_gpb, int, int> l_el;
        std::minstd_rand rnd(seed);
        el = &l_el;
        long long res = start_threads(4, rnd, el_set_test);
        std::cout << "bronson      "<< (double)sh_res/res << ' ' << res << std::endl;
    }
    cds::Terminate();
}

void test_stress(){

    ConcurrentAVL_LO<int>* lo_set;
    auto lo_set_test = [&](int seed, int& res){
        res = stress(*lo_set, seed, 20, 1000000, 50, 50);
    };
    std::minstd_rand rnd(time(NULL));
    int last_i = 0;
    for(int i=0;i<300000;i++)
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        int res = start_threads(4, rnd, lo_set_test);
        if(i/10 != last_i) {
            std::cout << i << " done " << res <<  std::endl;
            last_i = i/10;
        }
        l_set.traverse_all();
        l_set.traverse_all_reverse();

        cds::Terminate();
    }
}
template<typename Container>
void start_iterator(Container& container, int start_delay, int step_delay, int count){
    cds::threading::Manager::attachThread();
    {
        int c = 0;
        auto it = container.begin();
        std::this_thread::sleep_for(std::chrono::milliseconds(start_delay));
        while(c< count) {
            if(it==container.begin()) {
                int last = -1;
                while (it != container.end()) {
                    if(step_delay > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(step_delay));
                    }
                    ++it;
                    c++;
                    int value = it.get();
                    if (value <= last) {
                        std::cout << "!!!error!!!" << std::endl;
                        throw 123;
                    }
                    last = value;
                }
            }else{
                int last = 1000000000;
                while (it != container.begin()) {
                    if(step_delay > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(step_delay));
                    }
                    --it;
                    c++;
                    int value = it.get();
                    if (value >= last) {
                        std::cout << "!!!error!!!" << std::endl;
                        throw 123;
                    }
                    last = value;
                }
            }
        }
    }
    cds::threading::Manager::detachThread();
}
void test_iterator(){
    ConcurrentAVL_LO<int>* lo_set;
    auto lo_set_test = [&](int seed, int& res){
        res = stress(*lo_set, seed, 100, 10000000, 50, 50);
    };
    std::minstd_rand rnd(time(NULL));
    int last_i = 0;
    for(int i=0;i<100;i++)
    {
        cds::Initialize();
        cds::gc::HP gc(8, 4, 64);
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        auto t1 = std::thread([&](){
            start_threads(4, rnd, lo_set_test);
            std::cout << "end1_";
        });
        start_threads(1, [&](){start_iterator(l_set, 100, 0, 70000000);});
        std::cout << "end2_";
        t1.join();
        std::cout <<i << ' ' << std::endl;
        cds::Terminate();
    }
}
int main() {
//    test_time(4, 4, 100000000, 5000000, 100, 0);
//    test_time(4, 1000, 10000000, 50, 50);
    test_stress();
//    test_iterator();
}

