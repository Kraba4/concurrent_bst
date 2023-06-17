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
#include "ConcurrentAVL_LO.h"
using namespace std::chrono_literals;
typedef cds::urcu::gc< cds::urcu::general_buffered<> >  rcu_gpb;

template<typename Container, int RANGE, long long MILLISECONDS>
int randCommands(Container& container, int seed, int p_insert, int p_erase){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    int c = 0;
    int fake = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<1000000000;++i){
        int r = rnd()%100;
        if(r <p_insert){
//            container.insert(i*seed);
            container.insert(rnd()%RANGE);
        }else if(r < p_insert+p_erase){
            container.erase(rnd()%RANGE);
        }else{
            if(container.contains(rnd()%RANGE)){
                fake++;
            };
        }
        c++;
        auto end=std::chrono::high_resolution_clock::now();
        if((end - start).count() > 1'000'000 * MILLISECONDS) break;
    }
    cds::threading::Manager::detachThread();
    return c + fake%2;
}
template<int RANGE, long long MILLISECONDS>
int randCommands(cds::container::BronsonAVLTreeMap<rcu_gpb, int, int>& container, int seed, int p_insert, int p_erase){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    int c = 0;
    int fake = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<1000000000;++i){
        int r = rnd()%100;
        if(r <p_insert){
//            container.insert(i*seed);
            container.insert(rnd()%RANGE);
        }else if(r < p_insert+p_erase){
            container.erase(rnd()%RANGE);
        }else{
            if(container.contains(rnd()%RANGE)){
                fake++;
            };
        }
        c++;
        auto end=std::chrono::high_resolution_clock::now();
        if((end - start).count() > 1'000'000 * MILLISECONDS) break;
    }
    cds::threading::Manager::detachThread();
    return c + fake%2;
}
template<int RANGE, long long MILLISECONDS>
int randCommands(std::set<int>& set, int seed, std::shared_mutex& mtx, int p_insert, int p_erase){
    std::minstd_rand rnd(seed);
//    std::lock_guard lock(mtx);
    int c = 0;
    int fake = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<1000000000;++i){
        int r = rnd()%100;
        if(r <p_insert){
            std::lock_guard lock(mtx);
            set.insert(rnd()%RANGE);
        }else if(r < p_insert + p_erase){
            std::lock_guard lock(mtx);
            set.erase(rnd()%RANGE);
        }else{
            std::shared_lock lock(mtx);
            if(set.find(rnd()%RANGE)!=set.end()){
                fake++;
            }
        }
        c++;
        auto end=std::chrono::high_resolution_clock::now();
        if((end - start).count() > 1'000'000 * MILLISECONDS) break;
    }
    return c + fake%2;
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
int start_threads(int n_threads,std::minstd_rand& rnd,  Func f){
    int seed = rnd()%1000000;
    std::minstd_rand local_rnd(seed);
    std::vector<std::thread> threads(n_threads);
    std::vector<int> results(n_threads);
    for(int i=0;i<n_threads;++i){
        threads[i] = std::thread(f, local_rnd(), std::ref(results[i]));
    }
    int sum = 0;
    for(int i=0;i<n_threads;++i){
        threads[i].join();
        sum+=results[i];
    }
    return sum;
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
    randCommands<Container, 1000000, 5000>(container, 1, 100, 0);
    cds::threading::Manager::detachThread();
}
void test_time(){

    std::set<int>* m_set;
    std::shared_mutex mtx;
    auto sm_set_test = [&](int seed, int& res){
        res = randCommands<1000, 2000LL>(*m_set, seed, mtx, 50, 50);
    };
    ConcurrentAVL_LO<int>* lo_set;
    auto lo_set_test = [&](int seed, int& res){
        res = randCommands<ConcurrentAVL_LO<int>, 1000, 2000LL>(*lo_set, seed, 50, 50);
    };
    ConcurrentPartiallyExternalTree<int>* co_set;
    auto co_set_test = [&](int seed, int& res){
        res = randCommands<ConcurrentPartiallyExternalTree<int>, 1000, 2000LL>(*co_set, seed, 50, 50);
    };

    std::minstd_rand rnd(time(NULL));
    int sh_res;
    std::cout << "\nthreads: " << 1 << std::endl;
    {
        std::set<int> mutex_set;
        m_set = &mutex_set;
        sh_res = start_threads(1, rnd, sm_set_test);
        std::cout << "shared_mutex " << 1 << ' ' <<  sh_res << std::endl;
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        int res = start_threads(1, rnd, lo_set_test);
        std::cout << "logic_order (1) " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        int res = start_threads(1, rnd, lo_set_test);
        std::cout << "logic_order  " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
        int res = start_threads(1, rnd, co_set_test);
        std::cout << "partially    " << (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }
//    {
//        cds::Initialize();
//        rcu_gpb gpbRCU;
//        cds::container::BronsonAVLTreeMap<rcu_gpb, int, int> el;
//        auto el_set_test = [&](int seed){
//            int res = randCommands< cds::container::BronsonAVLTreeMap<rcu_gpb, int, int>, 1000000, 5000>(el, seed);
//            return res;
//        };
//        int res = start_threads(1, rnd, el_set_test);
//        std::cout << "bronson      "<< (double)res/sh_res << ' ' << res << std::endl;
//        cds::Terminate();
//    }
    std::cout << "\nthreads: " << 2 << std::endl;
    {
        std::set<int> mutex_set;
        m_set = &mutex_set;
        sh_res = start_threads(2, rnd, sm_set_test);
        std::cout << "shared_mutex " << sh_res << std::endl;
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        init(l_set);
        int res = start_threads(2, rnd, lo_set_test);
        std::cout << "logic_order (1) " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        int res = start_threads(2, rnd, lo_set_test);
        std::cout << "logic_order  " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }

    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
        init(l_set);
        int res = start_threads(2, rnd, co_set_test);
        std::cout << "partially (i)    "  << (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
//        init(l_set);
        int res = start_threads(2, rnd, co_set_test);
        std::cout << "partially    "  << (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }

    std::cout << "\nthreads: " << 3 << std::endl;
    {
        std::set<int> mutex_set;
        m_set = &mutex_set;
        sh_res = start_threads(3, rnd, sm_set_test);
        std::cout << "shared_mutex  " << 1 << ' ' << sh_res << std::endl;
    }

    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        init(l_set);
        int res = start_threads(3, rnd, lo_set_test);
        std::cout << "logic_order (i)  " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        int res = start_threads(3, rnd, lo_set_test);
        std::cout << "logic_order   " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
        init(l_set);
        int res = start_threads(3, rnd, co_set_test);
        std::cout << "partially (i)    " << (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
//        init(l_set);
        int res = start_threads(3, rnd, co_set_test);
        std::cout << "partially     " << (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }
    std::cout << "\nthreads: " << 4 << std::endl;
    {
        std::set<int> mutex_set;
        m_set = &mutex_set;
        sh_res = start_threads(4, rnd, sm_set_test);
        std::cout << "shared_mutex  " << 1 << ' ' << sh_res << std::endl;
    }

    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        init(l_set);
        int res = start_threads(4, rnd, lo_set_test);
        std::cout << "logic_order (1) " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentAVL_LO<int> l_set;
        lo_set = &l_set;
        cds::threading::Manager::attachThread();
        l_set.insert(-10);
        cds::threading::Manager::detachThread();
        int res = start_threads(4, rnd, lo_set_test);
        std::cout << "logic_order  " << (double)res/sh_res << ' ' << res << std::endl;
//        std::cout << l_set.retries << std::endl;
        cds::Terminate();
    }

    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
        init(l_set);
        int res = start_threads(4, rnd, co_set_test);
        std::cout << "partially (i) "<< (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }
    {
        cds::Initialize();
        cds::gc::HP gc;
        ConcurrentPartiallyExternalTree<int> l_set;
        co_set = &l_set;
//        init(l_set);
        int res = start_threads(4, rnd, co_set_test);
        std::cout << "partially    "<< (double)res/sh_res << ' ' << res << std::endl;
        cds::Terminate();
    }

    cds::container::BronsonAVLTreeMap<rcu_gpb, int, int> el;
    cds::Initialize();
    rcu_gpb gpbRCU;
    {
        auto el_set_test = [&](int seed, int& res){
            res = randCommands<1000, 2000LL>(el, seed, 50, 50);
        };
        int res = start_threads(4, rnd, el_set_test);
        std::cout << "bronson      "<< (double)res/sh_res << ' ' << res << std::endl;
    }
    cds::Terminate();
}
void test_stress(){

    ConcurrentAVL_LO<int>* lo_set;
    auto lo_set_test = [&](int seed, int& res){
        res = stress(*lo_set, seed, 1000, 100000, 50, 30);
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
        if(i/1000 != last_i) {
            std::cout << i << " done " << res <<  std::endl;
            last_i = i/1000;
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
            start_threads(64, rnd, lo_set_test);
            std::cout << "end1_";
        });
        start_threads(8, [&](){start_iterator(l_set, 100, 0, 370000000);});
        std::cout << "end2_";
        t1.join();
        std::cout <<i << ' ' << std::endl;
        cds::Terminate();
    }
}
int main() {
//    test_time();
    test_stress();
//    test_iterator();
}

