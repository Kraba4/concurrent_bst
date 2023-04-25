#include <iostream>
#include "ConcurrentPartiallyExternalTree.h"
int main() {
    ConcurrentPartiallyExternalTree<int> tree;
    tree.insert(5);
    tree.insert(3);
    tree.insert(8);

    tree.remove(8);
    tree.remove(5);
    tree.remove(3);
    tree.contains(5);
    return 0;
}
