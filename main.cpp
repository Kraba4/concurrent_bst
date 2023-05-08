#include <iostream>
#include "ConcurrentPartiallyExternalTree.h"
#include "Old.h"
#include <set>
#include <atomic>
#include <thread>
#include <random>
void randCommands(ConcurrentPartiallyExternalTree<int>& tree, int seed){
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
        if(r <30){
            tree.insert(rnd()%100000);
        }else if(r < 35){
            tree.remove(rnd()%100000);
        }else{
            tree.contains(rnd()%100000);
        }
    }
}
void randCommands(std::set<int>& set, int seed, std::mutex& mtx){
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
        if(r <30){
            std::lock_guard lock(mtx);
            set.insert(rnd()%100000);
        }else if(r < 35){
            std::lock_guard lock(mtx);
            set.erase(rnd()%100000);
        }else{
            std::lock_guard lock(mtx);
            set.contains(rnd()%100000);
        }
    }
}

void randCommands(ConcurrentPartiallyExternalTreeOld<int>& set, int seed, std::mutex& mtx){
    std::minstd_rand rnd(seed);
    for(int i=0;i<1000000;++i){
        int r = rnd()%100;
        if(r <30){
            std::lock_guard lock(mtx);
            set.insert(rnd()%100000);
        }else if(r < 35){
            std::lock_guard lock(mtx);
            set.remove(rnd()%100000);
        }else{
            std::lock_guard lock(mtx);
            set.contains(rnd()%100000);
        }
    }
}
int main() {
    ConcurrentPartiallyExternalTree<int> tree;
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
std::cout << "end;";
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
