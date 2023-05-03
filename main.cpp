#include <iostream>
#include "ConcurrentPartiallyExternalTree.h"
#include <set>
#include <atomic>


int main() {
//    std::cout << a. << std::endl;
//    std::cout << sizeof(a) << std::endl;

   ConcurrentPartiallyExternalTree<int> tree;
//   std::cout << sizeof(tree) << std::endl;
   tree.insert(5);
//   std::atomic<std::shared_ptr<int>> r;

//   r.load();
//   int* a;
//   std::cout << sizeof(a);
    tree.insert(3);
//    tree.insert(8);
//
//    tree.remove(8);
//    tree.remove(5);
//    tree.remove(3);
//    tree.contains(8);
std::cout << "end;";
    return 0;
}
void* operator new(size_t s){
    std::cout <<"!"<< s <<"!" << std::endl;
    void* t = malloc(s);
    std::cout << "new " << t << std::endl;
    return malloc(s);
}

void operator delete (void* data){
    std::cout <<"deleted " << data << std::endl;
}