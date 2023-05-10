#include <iostream>
#include "ConcurrentPartiallyExternalTree.h"
#include "Old.h"
#include <set>
#include <atomic>
#include <thread>
#include <random>
#include <mutex>
#include <cds/init.h>
#include <cds/threading/model.h>
//#include <cds/gc/hp.h>
//#include "lfmap_avl.h"
void randCommands(ConcurrentPartiallyExternalTree<int>& tree, int seed){
    cds::threading::Manager::attachThread();
    std::minstd_rand rnd(seed);
    for(int i=0;i<10000;++i){
        int r = rnd()%100;
        if(r <200){
            tree.insert(rnd()%100000000);
        }else if(r < 35){
            tree.remove(rnd()%100000);
        }else{
            tree.contains(rnd()%1000000);
        }
    }
    cds::threading::Manager::detachThread();
}
void randCommands(std::set<int>& set, int seed, std::mutex& mtx){
    std::minstd_rand rnd(seed);
    for(int i=0;i<10000;++i){
        int r = rnd()%100;
        if(r <200){
            std::lock_guard lock(mtx);
            set.insert(rnd()%100000000);
        }else if(r < 20){
            std::lock_guard lock(mtx);
            set.erase(rnd()%100000);
        }else{
            std::lock_guard lock(mtx);
            set.find(rnd()%1000000);
        }
    }
}

void randCommands(ConcurrentPartiallyExternalTreeOld<int>& set, int seed, std::mutex& mtx){
    std::minstd_rand rnd(seed);
    for(int i=0;i<10000;++i){
        int r = rnd()%100;
        if(r <200){
            std::lock_guard lock(mtx);
            set.insert(rnd()%100000000);
        }else if(r < 20){
            std::lock_guard lock(mtx);
            set.remove(rnd()%100000);
        }else{
            std::lock_guard lock(mtx);
            set.contains(rnd()%1000000);
        }
    }
}

//void randCommands(LFStructs::LFMapAvl<int,int>& set, int seed){
//    std::minstd_rand rnd(seed);
//    for(int i=0;i<100000;++i){
////        std::cout << i<<std::endl;
//        int r = rnd()%100;
//        if(r <90){
//            set.upsert(rnd()%100000,rnd());
//        }else if(r < 20){
//            set.remove(rnd()%100000);
//        }else{
//            set.get(rnd()%100000);
//        }
//    }
//}
int main() {
    std::minstd_rand rnd;
    int seed1 = 271;//rnd()%1000;
    int seed2 = 794;//rnd()%1000;
    int seed3 = 886;//rnd()%1000;
    int seed4 = 637;//rnd()%1000;
    std::cout << seed1 << ' ' << seed2 << ' ' << seed3 << ' ' << seed4 << std::endl;
    std::set<int> mutex_set;
    std::mutex mtx;
    auto start = std::chrono::high_resolution_clock::now();

    std::thread tq1([&](){
        randCommands(mutex_set,seed1,mtx);
    });
    std::thread tq2([&](){
        randCommands(mutex_set,seed2,mtx);
    });
    std::thread tq3([&](){
        randCommands(mutex_set,seed3,mtx);
    });
    std::thread tq4([&](){
        randCommands(mutex_set,seed4,mtx);
    });
    tq1.join();
    tq2.join();
    tq3.join();
    tq4.join();
    auto end = std::chrono::high_resolution_clock::now();
    constexpr double BILLION = 1'000'000'000;
    std::cout << (end - start).count()/BILLION << '\n';

    cds::Initialize() ;
    cds::gc::HP gc;
    ConcurrentPartiallyExternalTree<int> tree;
    start = std::chrono::high_resolution_clock::now();

    std::thread t1([&](){
        randCommands(tree, seed1);
    });
    std::thread t2([&](){
        randCommands(tree, seed2);
    });
    std::thread t3([&](){
        randCommands(tree, seed3);
    });
    std::thread t4([&](){
        randCommands(tree, seed4);
    });
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    end = std::chrono::high_resolution_clock::now();
    std::cout << (end - start).count()/BILLION << '\n';
    cds::Terminate() ;
    ConcurrentPartiallyExternalTreeOld<int> e;
    start = std::chrono::high_resolution_clock::now();
    std::thread tqw1([&](){
        randCommands(e,seed1,mtx);
    });
    std::thread tqw2([&](){
        randCommands(e,seed2,mtx);
    });
    std::thread tqw3([&](){
        randCommands(e,seed3,mtx);
    });
    std::thread tqw4([&](){
        randCommands(e,seed4,mtx);
    });
    tqw1.join();
    tqw2.join();
    tqw3.join();
    tqw4.join();
    end = std::chrono::high_resolution_clock::now();
    std::cout << (end - start).count()/BILLION << '\n';



//    LFStructs::LFMapAvl<int,int> e1;
//    start = std::chrono::high_resolution_clock::now();
//    std::thread tqwe1([&](){
//        randCommands(e1,seed1);
//    });
//    std::thread tqwe2([&](){
//        randCommands(e1,seed2);
//    });
//    std::thread tqwe3([&](){
//        randCommands(e1,seed3);
//    });
//    std::thread tqwe4([&](){
//        randCommands(e1,seed4);
//    });
//    tqwe1.join();
//    tqwe2.join();
//    tqwe3.join();
//    tqwe4.join();
//    end = std::chrono::high_resolution_clock::now();
//    std::cout << (end - start).count()/BILLION << '\n';

std::cout << "end;";
std::cout << "retries(" << tree.retries << ")";
tree.print();
    std::cout << "retries(" << tree.retries << ")";
    return 0;
}
void* operator new(size_t s){
//    std::cout <<"!"<< s <<"!" << std::endl;
    void* t = malloc(s);
//    std::cout << "new " << t << std::endl;
    return t;
}

void operator delete (void* data, std::size_t size){
//    std::cout <<"delete " << data << std::endl;
    free(data);
}
