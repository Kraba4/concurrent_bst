#include <iostream>
#include "ConcurrentPartiallyExternalTree.h"
#include "Old.h"
#include "ConcurrentAVL.h"
#include <set>
#include <atomic>
#include <thread>
#include <random>
#include <mutex>
#include <cds/init.h>
#include <cds/threading/model.h>
#include <cds/container/bronson_avltree_map_rcu.h>
#include <cds/urcu/general_buffered.h>
typedef cds::urcu::gc< cds::urcu::general_buffered<> >  rcu_gpb;

void randCommands(cds::container::BronsonAVLTreeMap<rcu_gpb, int, int>& tree, int seed, int& a){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
//        tree.insert(rnd()%1000000);
        if(r <80){
            tree.emplace(rnd()%1000000, r);
        }else if(r < 35){
            tree.erase(rnd()%1000000);
        }else{
            if(tree.contains(rnd()%1000000)){
                a++;
            };
        }
    }
    cds::threading::Manager::detachThread();
}

void randCommands(ConcurrentPartiallyExternalTree<int>& tree, int seed, int& a){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
//        tree.insert(rnd()%1000000);
        if(r <80){
            tree.insert(rnd()%1000000);
        }else if(r < 35){
            tree.erase(rnd()%1000000);
        }else{
            if(tree.contains(rnd()%1000000)){
                a++;
            };
        }
    }
    cds::threading::Manager::detachThread();
}
void randCommands(ConcurrentAVL<int>& tree, int seed, int& a){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
//        tree.insert(rnd()%1000000);
        if(r <80){
            tree.insert(rnd()%1000000);
        }else if(r < 20){
            tree.erase(rnd()%1000000);
        }else{
            if(tree.contains(rnd()%1000000)){
                a++;
            };
        }
    }
    cds::threading::Manager::detachThread();
}
void randCommands(std::set<int>& set, int seed, std::mutex& mtx, int& a){
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
        if(r <80){
            std::lock_guard lock(mtx);
            set.insert(rnd()%1000000);
        }else if(r < 35){
            std::lock_guard lock(mtx);
            set.erase(rnd()%1000000);
        }else{
            std::lock_guard lock(mtx);
            if(set.find(rnd()%1000000)!=set.end()){
                a++;
            }
        }
    }
}

void randCommands(ConcurrentPartiallyExternalTreeOld<int>& set, int seed, std::mutex& mtx, int& a){
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
        if(r <35){
            std::lock_guard lock(mtx);
            set.insert(rnd()%1000000);
        }else if(r < 35){
            std::lock_guard lock(mtx);
            set.remove(rnd()%1000000);
        }else{
            std::lock_guard lock(mtx);
            if(set.contains(rnd()%1000000)){
                a++;
            }
        }
    }
//    std::cout << a;
}
void test(ConcurrentAVL<int>& q){
    cds::threading::Manager::attachThread();
    q.insert(5);
    q.insert(7);
//    q.erase(5);
    q.insert(8);
    q.insert(9);
    q.insert(10);
    q.insert(11);
    q.contains(5);

    cds::threading::Manager::detachThread();
}
void test2(ConcurrentAVL<int>& q){
    cds::threading::Manager::attachThread();
    q.insert(3);
    q.insert(8);
    q.insert(12);
    q.insert(6);

    cds::threading::Manager::detachThread();
}
void init(ConcurrentAVL<int>& q){
    cds::threading::Manager::attachThread();
    for(int j=0;j<200000; ++j){
        q.insert(j);
    }
    cds::threading::Manager::detachThread();
}
int main() {
    std::minstd_rand rnd(time(NULL));
    int seed1 = rnd()%1000;//271;//rnd()%1000;
    int seed2 = rnd()%1000;//794;//rnd()%1000;
    int seed3 = rnd()%1000;//886;//rnd()%1000;
    int seed4 = rnd()%1000;//637;//rnd()%1000;
    cds::Initialize() ;
    {
        cds::gc::HP gc(4, 4, 32);
        int a = 0, a2=0,a3=0,a4=0;
        auto start = std::chrono::high_resolution_clock::now();
//        for(int i=0;i<1000;++i) {
//            ConcurrentAVL<int> q;
//            init(q);
//            std::thread t([&]() {
//                randCommands(q, seed1, a);
//            });
//            std::thread t2([&](){
//                randCommands(q, seed2, a2);
//            });
//            t.join();
//            t2.join();
//            q.check_heights();
//            std::cout << i<< ' ' <<  q.false_balance << std::endl;
//        }
        ConcurrentAVL<int> q;
        std::thread t([&]() {
            randCommands(q, seed1, a);
        });
        std::thread t2([&](){
            randCommands(q, seed2, a2);
        });
        std::thread t3([&](){
            randCommands(q, seed3, a3);
        });
        std::thread t4([&](){
            randCommands(q, seed4, a4);
        });
        t.join();
        t2.join();
        t3.join();
        t4.join();
        auto end = std::chrono::high_resolution_clock::now();
        constexpr double BILLION = 1'000'000'000;
        std::cout << (end - start).count()/BILLION << '\n';
//        q.check_heights();
//        std::cout << q.false_balance << std::endl;
//        q.print();
    }
    cds::Terminate();
    std::cout << seed1 << ' ' << seed2 << ' ' << seed3 << ' ' << seed4 << std::endl;
    std::set<int> mutex_set;
    std::mutex mtx;
    int c1=0,c2=0,c3=0,c4=0;
    auto start = std::chrono::high_resolution_clock::now();

    std::thread tq1([&](){
        randCommands(mutex_set,seed1,mtx,c1);
    });
    std::thread tq2([&](){
        randCommands(mutex_set,seed2,mtx,c2);
    });
    std::thread tq3([&](){
        randCommands(mutex_set,seed3,mtx,c3);
    });
    std::thread tq4([&](){
        randCommands(mutex_set,seed4,mtx,c4);
    });
    tq1.join();
    tq2.join();
    tq3.join();
    tq4.join();
    auto end = std::chrono::high_resolution_clock::now();
    constexpr double BILLION = 1'000'000'000;
    std::cout << (end - start).count()/BILLION << '\n';

    cds::Initialize() ;
    {
        cds::gc::HP gc(4, 4, 32);
        ConcurrentPartiallyExternalTree<int> tree;
        int b1 = 0, b2 = 0, b3 = 0, b4 = 0;
        start = std::chrono::high_resolution_clock::now();

        std::thread t1([&]() {
            randCommands(tree, seed1, b1);
        });
        std::thread t2([&]() {
            randCommands(tree, seed2, b2);
        });
        std::thread t3([&]() {
            randCommands(tree, seed3, b3);
        });
        std::thread t4([&]() {
            randCommands(tree, seed4, b4);
        });
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        end = std::chrono::high_resolution_clock::now();
        std::cout << (end - start).count() / BILLION << '\n';
    }
    cds::Terminate() ;

    cds::Initialize() ;
    {
        rcu_gpb gpbRCU;
        cds::container::BronsonAVLTreeMap<rcu_gpb, int, int> el;
        int b11 = 0, b21 = 0, b31 = 0, b41 = 0;
        start = std::chrono::high_resolution_clock::now();

        std::thread t11([&]() {
            randCommands(el, seed1, b11);
        });
        std::thread t21([&]() {
            randCommands(el, seed2, b21);
        });
        std::thread t31([&]() {
            randCommands(el, seed3, b31);
        });
        std::thread t41([&]() {
            randCommands(el, seed4, b41);
        });
        t11.join();
        t21.join();
        t31.join();
        t41.join();
        end = std::chrono::high_resolution_clock::now();
        std::cout << (end - start).count() / BILLION << '\n';
        int x;
        std::cin >> x;
    }
    cds::Terminate() ;
    int a1 =0, a2=0,a3=0,a4=0;
    ConcurrentPartiallyExternalTreeOld<int> e;
    start = std::chrono::high_resolution_clock::now();
    std::thread tqw1([&](){
        randCommands(e,seed1,mtx,a1);
    });
    std::thread tqw2([&](){
        randCommands(e,seed2,mtx,a2);
    });
    std::thread tqw3([&](){
        randCommands(e,seed3,mtx,a3);
    });
    std::thread tqw4([&](){
        randCommands(e,seed4,mtx,a4);
    });
    tqw1.join();
    tqw2.join();
    tqw3.join();
    tqw4.join();
    end = std::chrono::high_resolution_clock::now();
    std::cout << (end - start).count()/BILLION << '\n';


//
////    LFStructs::LFMapAvl<int,int> e1;
////    start = std::chrono::high_resolution_clock::now();
////    std::thread tqwe1([&](){
////        randCommands(e1,seed1);
////    });
////    std::thread tqwe2([&](){
////        randCommands(e1,seed2);
////    });
////    std::thread tqwe3([&](){
////        randCommands(e1,seed3);
////    });
////    std::thread tqwe4([&](){
////        randCommands(e1,seed4);
////    });
////    tqwe1.join();
////    tqwe2.join();
////    tqwe3.join();
////    tqwe4.join();
////    end = std::chrono::high_resolution_clock::now();
////    std::cout << (end - start).count()/BILLION << '\n';
//
//std::cout << "end;";
////std::cout << "retries(" << tree.retries << ")";
////tree.print();
////    std::cout << "retries(" << tree.retries << ")";
//    return 0;
}
//void* operator new(size_t s){
////    std::cout <<"!"<< s <<"!" << std::endl;
//    void* t = malloc(s);
////    std::cout << "new " << t << std::endl;
//    return t;
//}
//
//void operator delete (void* data, std::size_t size){
////    std::cout <<"delete " << data << std::endl;
//    free(data);
//}
