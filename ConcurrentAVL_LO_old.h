//
// Created by HONOR on 01.06.2023.
//

#ifndef BACHELOR_CONCURRENTAVL_LO_OLD_H
#define BACHELOR_CONCURRENTAVL_LO_OLD_H
#include <functional>
#include <memory>
#include <mutex>
#include <cds/sync/spinlock.h>
#include <cds/gc/hp.h>
#define mor std::memory_order_relaxed
template <typename Key, typename Compare = std::less<Key>, typename Alloc = std::allocator<Key>>
class ConcurrentAVL_LO_old{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key key;
        int height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
        std::atomic<Node*> p;
        std::atomic<Node*> next;
        std::atomic<Node*> prev;
        cds::sync::spin_lock<cds::backoff::LockDefault> mtx;
        cds::sync::spin_lock<cds::backoff::LockDefault> lo_mtx;

        bool isUnlinked(){
            return next.load(mor)==nullptr;
        }
        void unlink(){
            prev.store(nullptr, mor);
            next.store(nullptr, mor);
            l.store(nullptr, mor);
            r.store(nullptr, mor);
            p.store(nullptr, mor);
        }
        Node(const Key& key, Node* parent): l(), r(), p(parent),
                                key(key), height(0){
        }
        Node(){}
        ~Node(){
//            std::cout << "destroy " << key << std::endl;
//            cds::gc::HP::retire<NodeDisposer>(l.load());
//            cds::gc::HP::retire<NodeDisposer>(r.load());
        }
        template<typename ...Args>
        static Node* createNode(Node_allocator& alloc, Args&&... args){
            Node* p = Alloc_traits::allocate(alloc, 1);
            Alloc_traits::construct(alloc, p, std::forward<Args>(args)...);
            return p;
        }
//        static void deleteNode(Node_allocator& alloc, LFStructs::SharedPtr<Node> node){
//            Alloc_traits::destroy(alloc, node);
//            Alloc_traits::deallocate(alloc, node, 1);
//        }
    };
    struct NodeDisposer{
        void operator()(Node* p){
            delete p;
        }
    };
    typename Node::Node_allocator alloc;
    static bool comp(const Key& a, const Key& b){
        return a < b;
    }
    std::atomic<Node*> header;
    std::mutex null_header_mtx;
    std::atomic<Node*> begin_node;
    std::atomic<Node*> end_node;
    Node* begin_ptr;
    Node* end_ptr;
    struct local_fields{
        Node** cached_node;
        cds::gc::HP::GuardArray<5>* ga;
    };
    std::unordered_map<std::thread::id, local_fields> local;
public:
    ConcurrentAVL_LO_old(){
        Node* begin = Node::createNode(alloc, -100000, nullptr);
        Node* end =Node::createNode(alloc, 2000000000, nullptr);
        begin_ptr = begin;
        end_ptr = end;
        begin->prev.store(end);
        begin->next.store(end);
        end->prev.store(begin);
        end->next.store(begin);
        begin_node.store(begin);
        end_node.store(end);
    }

    class iterator {
    private:
        int i = 0;
        long long step;
        cds::gc::HP::GuardArray<2> guarder;
        Node* node = nullptr;
        std::atomic<Node*>* header;
    public:
//        operator basenode* () { return node; }
        friend class ConcurrentAVL_LO_old;
        // Надо объявить вложенные типы итератора
//        typedef std::bidirectional_iterator_tag iterator_category;
//        typedef T key_type;
//        typedef int difference_type;
//        typedef T& reference;
//        typedef T* pointer;
//        iterator(basenode* nd) : node(nd) {}

        iterator(std::atomic<Node*>* header, std::atomic<Node*>* node)
        : header(header), node(guarder.protect(0, *node)), step(1){}

        iterator(std::atomic<Node*>* header){

        }
        iterator(const iterator& other){
            guarder.assign(0, other.guarder.get<Node>(0));
            guarder.assign(1, other.guarder.get<Node>(1));
            node = other.node;
            header = other.header;
        }
        iterator operator++() {
            i=0;
            Key key = node->key;
            Node* new_node = guarder.protect(step%2, node->next);
            step++;
            int c = 10;
            if(new_node == nullptr) {
                i = 1;
                cds::gc::HP::GuardArray<2> ga;
                while (new_node == nullptr || new_node->key <= key) {
                    new_node = simple_search(ga, *header, key);
                    if (new_node != nullptr) {
                        i += c*1;
                        c*=10;
                        if (new_node->key <= key) {
                            i += c*3;
                            c*=10;
                            new_node = guarder.protect(step%2, new_node->next);
                            step++;
                        } else {
                            i += c*4;
                            c*=10;
                            guarder.assign(step%2, new_node);
                            step++;
                        }
                    }else{
                        i += c*2;
                        c*=10;
                    }
                }
            }
            c = new_node->key;
            node = new_node;
            return *this;
        }
//        iterator operator++(int) {
//            basenode* t = node;
//            node = next(node);
//            return iterator(t);
//        }
        iterator operator--() {
            i=0;
            Key key = node->key;
            Node* new_node = guarder.protect(step%2, node->prev);
            step++;
            int c = 10;
            if(new_node == nullptr) {
                i = 1;
                cds::gc::HP::GuardArray<2> ga;
                while (new_node == nullptr || new_node->key >= key) {
                    new_node = simple_search(ga, *header, key);
                    if (new_node != nullptr) {
                        i += c*1;
                        c*=10;
                        if (new_node->key >= key) {
                            i += c*3;
                            c*=10;
                            new_node = guarder.protect(step%2, new_node->prev);
                            step++;
                        } else {
                            i += c*4;
                            c*=10;
                            guarder.assign(step%2, new_node);
                            step++;
                        }
                    }else{
                        i += c*2;
                        c*=10;
                    }
                }
            }
            c = new_node->key;
            node = new_node;
            return *this;
        }

//        iterator operator--(int) {
//            basenode* t = node;
//            node = prev(node);
//            return iterator(t);
//        }
        bool operator==(const iterator other) {
            return node == other.node;
        }
        bool operator!=(const iterator other) {
            return node != other.node;
        }
        void print(){
            std::cout << node->key << std::endl;
        }
        Key get(){
            return node->key;
        }
//        guarded_ptr<Node*> operator*() {
//            return nullptr;
//        }
    };
    iterator begin(){
        return iterator(&header, &begin_ptr->next);
    }
    iterator end(){
//        iterator(&header, end_ptr->prev);
        return iterator(&header, &end_node);
//        return iterator(&header, begin_ptr->next);
    }
private:
    static bool equiv(const Key& a, const Key& b){
        return !comp(a, b) && !comp(b, a);
    }
    static Node*
    simple_search(cds::gc::HP::GuardArray<2>& guard_array,
           const std::atomic<Node*>& root,
           const Key& key) {
        Node *parent = nullptr;
        int step = 0;
        Node *curr = guard_array.protect(step, root);
        while (curr) {
            if (equiv(curr->key, key)) {
                break;
            }
            step++;
            parent = curr;
            if (comp(key, curr->key)) {
                curr = guard_array.protect(step % 2, curr->l);
            } else {
                curr = guard_array.protect(step % 2, curr->r);
            }
        }
        if(curr) return curr;
        return parent;
    }

    template<int N>
    std::tuple<Node*, Node*>
    search(cds::gc::HP::GuardArray<N>& guard_array,
           const std::atomic<Node*>& root,
           const Key& key) const{
        Node* parent = nullptr;
        int step=0;
        Node* curr = guard_array.protect(step, root);

        while(curr){
            if(equiv(curr->key,key)){
                break;
            }
            step++;
            parent = curr;
            if(comp(key,curr->key)){
                curr = guard_array.protect(step%2,curr->l);
            }else{
                curr = guard_array.protect(step%2,curr->r);
            }
        }

        if(curr == nullptr) {
            step++;
            if (key < parent->key) {
                Node* new_parent = begin_ptr;
                while (key < parent->key) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->prev);
                    if(new_parent == begin_ptr) break;
                    if(new_parent == nullptr) return {parent, curr};
                    parent = new_parent;
                }
                if (new_parent != begin_ptr && parent->r.load(mor) != nullptr) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->next);
                    if(new_parent == nullptr) return {parent, curr};
                    if(new_parent != end_ptr){
                        parent = new_parent;
                    }
                }
            } else {
                Node* new_parent = end_ptr;
                while (key > parent->key) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->next);
                    if(new_parent == end_ptr) break;
                    if(new_parent == nullptr) return {parent, curr};
                    parent = new_parent;
                }
                if (new_parent != end_ptr && parent->l.load(mor) != nullptr) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->prev);
                    if(new_parent == nullptr) return {parent, curr};
                    if(new_parent != begin_ptr){
                        parent = new_parent;
                    }
                }
            }
            if(parent->key == key){
                curr = parent;
                parent = guard_array.protect(step%2, curr->p);
            }
        }

        return {parent, curr};
    }
    bool insert_left_child(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node*& cached_node){
        Node*& new_node = cached_node;
        {
                    std::lock_guard lock1(parent->mtx);
                    if (parent->isUnlinked()) return false;
                    if (parent->l.load(mor) !=nullptr) return false;
                    
                    while(true) {
                        Node* prev = guard_array.protect(2, parent->prev);
                        if (prev->key > new_node->key) return false;
                        if (prev->key == new_node->key) return true;
                        std::lock_guard lock2(prev->lo_mtx);
                        if (prev->next.load(mor) != parent) continue;
//                    if(prev->isUnlinked()) continue;

                        std::lock_guard lock3(parent->lo_mtx);
//                        Node *new_node = Node::createNode(alloc, key, parent);
                        new_node->prev.store(prev, mor); new_node->next.store(parent, mor);
                        prev->next.store(new_node, mor);
                        parent->prev.store(new_node, mor); parent->l.store(new_node, mor);
                        break;
                    }
        }
//        fix(parent);
        cached_node = nullptr;
        return true;
    }
    bool insert_right_child(cds::gc::HP::GuardArray<5>& guard_array,
                            Node* parent, Node*& cached_node) {
        Node*& new_node = cached_node;
        std::lock_guard lock1(parent->mtx);
        if(parent->isUnlinked()) return false;
        if(parent->r.load(mor) != nullptr) return false;

        std::lock_guard lock2(parent->lo_mtx);
        Node *next = guard_array.protect(2, parent->next);
        if(next->key < new_node->key) return false;
        if(next->key == new_node->key) return true;

        std::lock_guard lock3(next->lo_mtx);
//        if(parent->next.load(mor) != next) return false;
//        Node* new_node = Node::createNode(alloc, key, parent);
        new_node->prev.store(parent, mor); new_node->next.store(next, mor);
        parent->next.store(new_node, mor); next->prev.store(new_node, mor);
        parent->r.store(new_node, mor);

        cached_node = nullptr;
        return true;
    }

    bool insert_new_node(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node*& cached_node) {
        if (comp(cached_node->key, parent->key)) {
            return insert_left_child(guard_array, parent, cached_node);
        }else {
            return insert_right_child(guard_array, parent, cached_node);
        }
    }
public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
        if(header.load(mor) == nullptr){
            return false;
        }
        static thread_local cds::gc::HP::GuardArray<5>* ga = nullptr;
        if(ga==nullptr){
            if(local.contains(std::this_thread::get_id())){
                ga = local[std::this_thread::get_id()].ga;
            }else {
                ga = new cds::gc::HP::GuardArray<5>();
                local[std::this_thread::get_id()].ga = ga;
            }
        }
        auto [parent, curr] = search(*ga, header, key);
        return curr; //узел найден и он не помечен как удаленный, тогда true
    }

    void insert(const Key& key){
        static thread_local cds::gc::HP::GuardArray<5>* ga = nullptr;
        static thread_local Node* cached_node = nullptr;
        if(ga==nullptr){
            if(local.contains(std::this_thread::get_id())){
                ga = local[std::this_thread::get_id()].ga;
            }else {
                ga = new cds::gc::HP::GuardArray<5>();
                local[std::this_thread::get_id()].ga = ga;
            }
            local[std::this_thread::get_id()].cached_node = &cached_node;
        }
        while(true) {
            if (header.load(mor) == nullptr) {
                std::lock_guard lock(null_header_mtx);
                if(header.load(mor) != nullptr){ //пока брали блокировку, кто-то занял header
                    continue;
                }
                if(cached_node == nullptr) {
                    cached_node = Node::createNode(alloc, key, nullptr);
                }
                Node* begin = begin_node.load(mor);
                Node* end = end_node.load(mor);
                cached_node->prev.store(begin, mor);
                cached_node->next.store(end, mor);
                begin->next.store(cached_node, mor);
                end->prev.store(cached_node, mor);
                header.store(cached_node, mor);

                cached_node = nullptr;
                break;
            }

            auto [parent, curr] = search(*ga, header, key);
            if(curr!=nullptr){
                break;
            }

            if(cached_node == nullptr){
                cached_node = Node::createNode(alloc);
            }
            cached_node->key = key;
            cached_node->p.store(parent, mor);

            bool result = insert_new_node(*ga, parent, cached_node);
            if(result){
                break;
            }else{
//                ++retries;
            }
        }
    }

    bool erase_right_zero(cds::gc::HP::GuardArray<5>& guard_array,
                          Node* parent, Node* curr){
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->r.load(mor) != curr) return false; //right

            std::lock_guard lock2(curr->mtx);
            if (curr->isUnlinked()) return true;//сама удалилась
            if (curr->p.load(mor) != parent) return false; //not need mb
            int n_children = (curr->l.load(mor) != nullptr)
                             + (curr->r.load(mor) != nullptr);
            if (n_children != 0) return false;

            Node *next = guard_array.protect(2, curr->next); //right

            std::lock_guard lo_lock1(parent->lo_mtx);
            if (parent->next.load(mor) != curr) return false; //not need
            std::lock_guard lo_lock2(curr->lo_mtx);
            if (curr->prev.load(mor) != parent) return false; // not need
            std::lock_guard lo_lock3(next->lo_mtx);
            if (next->isUnlinked()) return false;
            if (next->prev.load(mor) != curr) return false;
            if (curr->next.load(mor) != next) return false;//not need?
            assert(parent->key < curr->key);
            assert(curr->key < next->key);
            curr->unlink();
//            curr->desc = 0;
            assert(next != curr);
            parent->next.store(next, mor);
            next->prev.store(parent, mor);// убрал из списка curr
            parent->r.store(nullptr, mor);
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
    bool erase_left_zero(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr) {
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->l.load(mor) != curr) return false; //left

            std::lock_guard lock2(curr->mtx);
            if (curr->isUnlinked()) return true;//удалился сам
            if (curr->p.load(mor) != parent) return false; //not need mb
            int n_children = (curr->l.load(mor) != nullptr)
                             + (curr->r.load(mor) != nullptr);
            if (n_children != 0) return false;

            Node *prev = guard_array.protect(2, curr->prev); //left

            std::lock_guard lo_lock1(prev->lo_mtx);
            if (prev->isUnlinked()) return false;
            if (prev->next.load(mor) != curr) return false;
            std::lock_guard lo_lock2(curr->lo_mtx);
            if (curr->next.load(mor) != parent) return false; // not need
            if (curr->prev.load(mor) != prev) return false;//not
            std::lock_guard lo_lock3(parent->lo_mtx);
            if (parent->prev.load(mor) != curr) return false; //not need
            assert(prev->key < curr->key);
            assert(curr->key < parent->key);
            curr->unlink();
//            curr->desc = 1;
//            curr->last_prev = prev;
//            curr->last_next = parent;
            parent->prev.store(prev, mor);
            assert(parent != curr);
            prev->next.store(parent, mor);// убрал из списка curr
            parent->l.store(nullptr, mor);
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
    bool erase_right_one(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr){
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->r.load(mor) != curr) return false; //right

            std::lock_guard lock2(curr->mtx);
            if (curr->isUnlinked()) return true;//удалился сам
            if (curr->p.load(mor) != parent) return false;//not
            Node *curr_l = curr->l.load(mor);
            Node *curr_r = curr->r.load(mor);
            if (curr_l != nullptr && curr_r == nullptr) {
                Node *next = guard_array.protect(2, curr->next);
                Node *prev = guard_array.protect(3, curr->prev);

                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->isUnlinked()) return false;
                if (prev->isUnlinked()) return false;
                if (curr->prev.load(mor) != prev) return false;
                if (next->prev.load(mor) != curr) return false;
                if (curr->next.load(mor) != next) return false; //not
                assert(prev->key < curr->key);
                assert(curr->key < next->key);
                curr->unlink();
//                curr->desc = 2;
//                curr->last_prev = prev;
//                curr->last_next = next;
                assert(curr != next);
                prev->next.store(next, mor);
                next->prev.store(prev, mor);//удалил из списка
                assert(prev->next.load(mor) == next);
                parent->r.store(curr_l, mor);
                curr_l->p.store(parent, mor);//удалил из дерева

            } else if (curr_l == nullptr && curr_r != nullptr) {
                Node *next = guard_array.protect(2, curr->next);
                std::lock_guard lo_lock1(parent->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->isUnlinked()) return false;
                if (curr->next.load(mor) != next) return false;
                if (next->prev.load(mor) != curr) return false;//not

                assert(parent->key < curr->key);
                assert(curr->key < next->key);
                curr->unlink();
//                curr->desc = 3;
//                curr->last_prev = parent;
                assert(curr != next);
                parent->next.store(next, mor);
                next->prev.store(parent, mor); //удалил из списка

                parent->r.store(curr_r, mor);
                curr_r->p.store(parent, mor);//удалил из дерева
            } else {
                return false;
            }
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
    bool erase_left_one(cds::gc::HP::GuardArray<5>& guard_array,
                        Node* parent, Node* curr) {
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->l.load(mor) != curr) return false; //left

            std::lock_guard lock2(curr->mtx);
            if (curr->isUnlinked()) return true;//удалился сам
            if (curr->p.load(mor) != parent) return false;//not
            Node *curr_l = curr->l.load(mor);
            Node *curr_r = curr->r.load(mor);
            if (curr_l != nullptr && curr_r == nullptr) {
                Node *prev = guard_array.protect(2, curr->prev);
                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(parent->lo_mtx);
                if (prev->isUnlinked()) return false;
                if (curr->prev.load(mor) != prev) return false;
                if (prev->next.load(mor) != curr) return false; //not

                assert(prev->key < curr->key);
                assert(curr->key < parent->key);
                curr->unlink();
//                curr->desc = 4;
//                curr->last_prev = prev;
//                curr->last_next = parent;
                assert(parent != curr);
                assert(prev != curr);
                parent->prev.store(prev, mor);
                prev->next.store(parent, mor);

                parent->l.store(curr_l, mor);
                curr_l->p.store(parent, mor);
            } else if (curr_l == nullptr && curr_r != nullptr) {
                Node *prev = guard_array.protect(2, curr->prev);
                Node *next = guard_array.protect(3, curr->next);

                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->isUnlinked()) return false;
                if (prev->isUnlinked()) return false;
                if (curr->prev.load(mor) != prev) return false;
                if (next->prev.load(mor) != curr) return false;
                if (curr->next.load(mor) != next) return false; //not

                assert(prev->key < curr->key);
                assert(curr->key < next->key);
                curr->unlink();
//                curr->desc = 5;
                next->prev.store(prev, mor);
                assert(next != curr);
                prev->next.store(next, mor);//удалил из списка

                parent->l.store(curr_r, mor);
                curr_r->p.store(parent, mor);//удалил из дерева
            } else {
                return false;
            }
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
    bool erase_right_two(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr){
//        return true;
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->r.load(mor) != curr) return false; //right

            std::lock_guard lock2(curr->mtx);
            if (curr->isUnlinked()) return true;//удалился сам
            if (curr->p.load(mor) != parent) return false;//not
            Node *curr_l = curr->l.load(mor);//deleted?
            Node *curr_r = curr->r.load(mor);
            int n_children = (curr_l != nullptr)
                             + (curr_r != nullptr);
            if (n_children != 2) return false;

            Node *next = guard_array.protect(2, curr->next);
            Node *prev = guard_array.protect(3, curr->prev);
            if (next == curr_r) {
                std::unique_lock lock3(next->mtx);
                if (next == end_ptr) lock3.unlock();

                if (next->l.load(mor) != nullptr) return false;
                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->isUnlinked()) return false;
                if (prev->isUnlinked()) return false;
                if (curr->next.load(mor) != next) return false;
                if (curr->prev.load(mor) != prev) return false;

                curr->unlink();

                prev->next.store(next, mor);
                next->prev.store(prev, mor);

                next->l.store(curr_l, mor);
                curr_l->p.store(next, mor);

                parent->r.store(next, mor);
                next->p.store(parent, mor);
            } else {
                Node *next_p = guard_array.protect(4, next->p);
                if (next_p == nullptr) return false;
                std::lock_guard lock3(next_p->mtx);
                if (next_p->isUnlinked()) return false;
                if (next_p->l.load(mor) != next) return false;
                std::lock_guard lock4(next->mtx);
                if (next->p.load(mor) != next_p) return false;
                if (next->l.load(mor) != nullptr) return false;

                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->isUnlinked()) return false;
                if (prev->isUnlinked()) return false;
                if (curr->next.load(mor) != next) return false;
                if (curr->prev.load(mor) != prev) return false;

                curr->unlink();

                prev->next.store(next, mor);
                next->prev.store(prev, mor);

                Node *next_r = next->r.load(mor);
                next_p->l.store(next_r, mor);
                if (next_r) next_r->p.store(next_p, mor);

                next->l.store(curr_l, mor);
                next->r.store(curr_r, mor);
                if (curr_l) curr_l->p.store(next, mor);
                if (curr_r) curr_r->p.store(next, mor);

                parent->r.store(next, mor);
                next->p.store(parent, mor);
            }
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }

    bool erase_left_two(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr) {
//        return true;
        {
        std::lock_guard lock1(parent->mtx);
        if (parent->isUnlinked()) return false;
        if (parent->l.load(mor) != curr) return false; //right

        std::lock_guard lock2(curr->mtx);
        if (curr->isUnlinked()) return true;//удалился сам
        if (curr->p.load(mor) != parent) return false;//not
        Node *curr_l = curr->l.load(mor);//deleted?
        Node *curr_r = curr->r.load(mor);
        int n_children = (curr_l != nullptr)
                         + (curr_r != nullptr);
        if (n_children != 2) return false;

        Node *next = guard_array.protect(2, curr->next);
        Node *prev = guard_array.protect(3, curr->prev);
        if (next == curr_r) {
//            std::lock_guard lock3(next->mtx);
            std::unique_lock lock3(next->mtx);
            if (next == end_ptr) lock3.unlock();

            if (next->l.load(mor) != nullptr) return false;
            std::lock_guard lo_lock1(prev->lo_mtx);
            std::lock_guard lo_lock2(curr->lo_mtx);
            std::lock_guard lo_lock3(next->lo_mtx);
            if (next->isUnlinked()) return false;
            if (prev->isUnlinked()) return false;
            if (curr->next.load(mor) != next) return false;
            if (curr->prev.load(mor) != prev) return false;

            curr->unlink();

            prev->next.store(next, mor);
            next->prev.store(prev, mor);

            next->l.store(curr_l, mor);
            curr_l->p.store(next, mor);

            parent->l.store(next, mor);
            next->p.store(parent, mor);
        } else {
            Node *next_p = guard_array.protect(4, next->p);
            if (next_p == nullptr || next_p->isUnlinked()) return false;
            std::lock_guard lock3(next_p->mtx);
            if (next_p->isUnlinked()) return false;
            if (next_p->l.load(mor) != next) return false;
            std::lock_guard lock4(next->mtx);
            if (next->p.load(mor) != next_p) return false;
            if (next->l.load(mor) != nullptr) return false;

            std::lock_guard lo_lock1(prev->lo_mtx);
            std::lock_guard lo_lock2(curr->lo_mtx);
            std::lock_guard lo_lock3(next->lo_mtx);
            if (next->isUnlinked()) return false;
            if (prev->isUnlinked()) return false;
            if (curr->next.load(mor) != next) return false;
            if (curr->prev.load(mor) != prev) return false;

            curr->unlink();

            prev->next.store(next, mor);
            next->prev.store(prev, mor);

            Node *next_r = next->r.load(mor);
            next_p->l.store(next_r, mor);
            if (next_r) { next_r->p.store(next_p, mor); }

            next->l.store(curr_l, mor);
            next->r.store(curr_r, mor);
            if (curr_l) curr_l->p.store(next, mor);
            if (curr_r) curr_r->p.store(next, mor);

            parent->l.store(next, mor);
            next->p.store(parent, mor);
        }
    }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
public:
//    std::atomic<int> retries = 0;
    bool erase(const Key& key){
        static thread_local cds::gc::HP::GuardArray<5>* ga = nullptr;
        if(ga==nullptr){
            if(local.contains(std::this_thread::get_id())){
                ga = local[std::this_thread::get_id()].ga;
            }else {
                ga = new cds::gc::HP::GuardArray<5>();
                local[std::this_thread::get_id()].ga = ga;
            }
        }
        while (true) {
            auto [parent, curr] = search(*ga,header, key);
            if (!curr) {
                return false; // узла нет в дереве, операция ни на что не повлияла
            }
            if(parent == nullptr){
                return true;
            }
//            if(parent == nullptr){
//                std::lock_guard lock(null_header_mtx);
//                if(curr == header.load()) {
//
//                }
//            }
            int n_children = (curr->l.load(mor) != nullptr)
                             + (curr->r.load(mor) != nullptr);
            bool result;
            if(comp(curr->key, parent->key)){
                if(n_children == 0) result = erase_left_zero(*ga, parent, curr);else
                if(n_children == 1) result = erase_left_one(*ga, parent, curr);else
                if(n_children == 2) result = erase_left_two(*ga, parent, curr);
            }else{
                if(n_children == 0) result = erase_right_zero(*ga, parent, curr);else
                if(n_children == 1) result = erase_right_one(*ga, parent, curr);else
                if(n_children == 2) result = erase_right_two(*ga, parent, curr);
            }
            if(result){
                break;
            }else{
//                ++retries;
            }
        }
        return true; // узел был, а в результате операции удален
    }
    int traverse_all_reverse(){
        Node* begin = begin_node.load();
        Node* curr = end_node.load()->prev;
        int c = 0;
        int co = 0;
        while(curr != begin){
            co++;
            Node* next = curr->prev.load();
            if(next == begin){
                break;
            }
            if(curr->key <= next->key){
                c++;
                throw "error";
            }
            curr = next;
        }
        if(s==0) check_heights();
//        std::cout << "order = " << c << " size " << (co == s) << " " << co << " " << s << std::endl;
        if(co != s || c!=0){
            return 1;
        }
        return 0;
    }
    Node* last_node;
    void traverse_all(Node* curr){
        if(curr == nullptr) return;

        traverse_all(curr->l.load());
        if(last_node->next.load() != curr){
            throw "123";
        }
        last_node = curr;
        traverse_all(curr->r.load());
    }
    void traverse_all(){
        last_node = begin_ptr;
        traverse_all(header.load());
        if(last_node->next.load() != end_ptr){
            throw "145";
        }
    }
    int s = 0;
    int last = -1000000;
    int er = 0;
    void check_heights(Node* curr){
        if(curr==nullptr){
            return;
        }
        check_heights(curr->l.load());
        if(curr->key <= last){
            er++;
        }
        last = curr->key;
        check_heights(curr->r.load());
//        curr->recalculate_height();

        s++;
    }
    void check_heights(){
        check_heights(header.load());
    }
};
#endif //BACHELOR_CONCURRENTAVL_LO_OLD_H
