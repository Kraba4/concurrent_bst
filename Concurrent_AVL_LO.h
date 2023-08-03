//
// Created by HONOR on 17.06.2023.
//

#ifndef BACHELOR_CONCURRENT_AVL_LO_H
#define BACHELOR_CONCURRENT_AVL_LO_H

#include <functional>
#include <memory>
#include <mutex>
#include <cds/sync/spinlock.h>
#include <cds/gc/hp.h>
#define mor std::memory_order_seq_cst
template <typename Key, typename Compare = std::less<Key>, typename Alloc = std::allocator<Key>>
class ConcurrentAVL_LO{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key key;
        std::atomic<int> height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
        std::atomic<Node*> p;
        std::atomic<Node*> next;
        std::atomic<Node*> prev;
        cds::sync::spin_lock<cds::backoff::LockDefault> mtx;
        cds::sync::spin_lock<cds::backoff::LockDefault> lo_mtx;
        std::atomic<bool> damaged;
        int last_balance;
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
        void update_children(){
            Node* L = l.load(mor);
            Node* R = r.load(mor);
            if(L){
                L->p.store(this);
            }
            if(R){
                R->p.store(this);
            }
        }
        static int get_height(Node* curr){
            if(curr == nullptr) return 0;
            return curr->height.load(mor);
        }
        void recalculate_height(){
            Node* L = l.load(mor);
            Node* R = r.load(mor);
            int lh = 0;
            int rh = 0;
            if(L != nullptr){
                lh = L->height.load(mor);
            }
            if(R != nullptr){
                rh = R->height.load(mor);
            }
            height.store(1 + std::max(lh, rh), mor);

        }
        int get_balance_factor(){
            Node* L = l.load(mor);
            Node* R = r.load(mor);
            int lh = 0;
            int rh = 0;
            if(L != nullptr){
                lh = L->height.load(mor);
            }
            if(R != nullptr){
                rh = R->height.load(mor);
            }
            return lh - rh;
        }

        Node(const Key& key, Node* parent): l(), r(), p(parent),
                                            key(key), height(1), damaged(true), last_balance(0){
        }
        Node(){}

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

//    std::atomic<Node*> header;
//    std::mutex null_header_mtx;
    std::atomic<Node*> begin_node;
    std::atomic<Node*> end_node;
    Node* begin_ptr;
    Node* end_ptr;
    std::atomic<Node*> header_parent;
//    Node* header_parent_ptr;
public:
    ConcurrentAVL_LO(){
        Node* begin = Node::createNode(alloc, -100000, nullptr);
        Node* end =Node::createNode(alloc, 2000000000, nullptr);
//        header_parent_ptr = Node::createNode(alloc);
//        header_parent.store(header_parent_ptr, mor);
        begin_ptr = begin;
        end_ptr = end;
        begin->prev.store(end);
        begin->next.store(end);
        end->prev.store(begin);
        end->next.store(begin);
        begin_node.store(begin);
        end_node.store(end);
    }
    ~ConcurrentAVL_LO(){
        Node* curr = begin_ptr;
        while(curr!=end_ptr){
            curr = curr->next.load(mor);
            delete curr->prev.load(mor);
        }
        delete curr;
    }
    class iterator {
    private:
        long long step;
        cds::gc::HP::GuardArray<2> guarder;
        Node* node = nullptr;
        Node* begin_ptr;
    public:
//        operator basenode* () { return node; }
        friend class ConcurrentAVL_LO;
        // Надо объявить вложенные типы итератора
//        typedef std::bidirectional_iterator_tag iterator_category;
//        typedef T key_type;
//        typedef int difference_type;
//        typedef T& reference;
//        typedef T* pointer;
//        iterator(basenode* nd) : node(nd) {}

        iterator(Node* begin_ptr, std::atomic<Node*>* node)
                : begin_ptr(begin_ptr), node(guarder.protect(0, *node)), step(1){}

        iterator(std::atomic<Node*>* header){

        }
        iterator(const iterator& other){
            guarder.assign(0, other.guarder.get<Node>(0));
            guarder.assign(1, other.guarder.get<Node>(1));
            node = other.node;
            begin_ptr = other.begin_ptr;
        }
        iterator operator++() {
            Key key = node->key;
            Node* new_node = guarder.protect(step%2, node->next);
            step++;
            if(new_node == nullptr) {
                cds::gc::HP::GuardArray<2> ga;
                while (new_node == nullptr || new_node->key <= key) {
                    new_node = simple_search(ga, begin_ptr, key);
                    if (new_node != nullptr) {
                        if (new_node->key <= key) {
                            new_node = guarder.protect(step%2, new_node->next);
                            step++;
                        } else {
                            guarder.assign(step%2, new_node);
                            step++;
                        }
                    }
                }
            }
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
                    new_node = simple_search(ga, begin_ptr, key);
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
        return iterator(begin_ptr, &begin_node);
    }
    iterator end(){
//        iterator(&header, end_ptr->prev);
        return iterator(begin_ptr, &end_node);
//        return iterator(&header, begin_ptr->next);
    }
private:
    static bool equiv(const Key& a, const Key& b){
        return !comp(a, b) && !comp(b, a);
    }
    static Node*
    simple_search(cds::gc::HP::GuardArray<2>& guard_array,
                  Node* begin_ptr,
                  const Key& key) {
        Node *parent = begin_ptr;
        int step = 0;
        Node *curr = guard_array.protect(step, begin_ptr->r);
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
        if(parent == begin_ptr) parent = nullptr;
        return parent;
    }

    template<int N>
    std::tuple<Node*, Node*>
    search(cds::gc::HP::GuardArray<N>& guard_array,
           const Key& key) const{
        Node* parent = begin_ptr;
        int step=0;
        Node* curr = guard_array.protect(step, parent->r);

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

        if(parent != begin_ptr && curr == nullptr) {
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
                if(parent == nullptr) parent = begin_ptr;
            }
        }

        return {parent, curr};
    }
    bool insert_left_child(cds::gc::HP::GuardArray<3>& guard_array,
                           Node* parent, const Key& key){
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->l.load(mor) != nullptr) return false;

            while (true) {
                Node *prev = guard_array.protect(2, parent->prev);
                if (prev->key > key) return false;
                if (prev->key == key) return true;
                std::lock_guard lock2(prev->lo_mtx);
                if (prev->next.load(mor) != parent) continue;

                std::lock_guard lock3(parent->lo_mtx);
                Node *new_node = Node::createNode(alloc, key, parent);
                std::lock_guard lock4(new_node->mtx);
                new_node->prev.store(prev, mor);
                new_node->next.store(parent, mor);
                prev->next.store(new_node, mor);
                parent->prev.store(new_node, mor);
                parent->l.store(new_node, mor);
                break;
            }
        }
        fix(guard_array, parent);
        return true;
    }
    bool insert_right_child(cds::gc::HP::GuardArray<3>& guard_array,
                            Node* parent, const Key& key) {
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->isUnlinked()) return false;
            if (parent->r.load(mor) != nullptr) return false;

            std::lock_guard lock2(parent->lo_mtx);
            Node *next = guard_array.protect(2, parent->next);
            if (next->key < key) return false;
            if (next->key == key) return true;

            std::lock_guard lock3(next->lo_mtx);
            Node *new_node = Node::createNode(alloc, key, parent);
            std::lock_guard lock4(new_node->mtx);
            new_node->prev.store(parent, mor);
            new_node->next.store(next, mor);
            parent->next.store(new_node, mor);
            next->prev.store(new_node, mor);
            parent->r.store(new_node, mor);
        }
        fix(guard_array, parent);
        return true;
    }

    bool insert_new_node(cds::gc::HP::GuardArray<3>& guard_array,
                         Node* parent, const Key& key) {
        if (comp(key, parent->key)) {
            return insert_left_child(guard_array, parent, key);
        }else {
            return insert_right_child(guard_array, parent, key);
        }
    }
    bool insert_header(const Key& key){
        std::lock_guard lock(begin_ptr->mtx);
        if(begin_ptr->r.load(mor) != nullptr) return false;

        Node* new_node =  Node::createNode(alloc, key, begin_ptr);
        std::lock_guard lock4(new_node->mtx);
        Node* begin = begin_node.load(mor);
        Node* end = end_node.load(mor);
        new_node->prev.store(begin, mor);
        new_node->next.store(end, mor);
        begin->next.store(new_node, mor);
        end->prev.store(new_node, mor);
        begin_ptr->r.store(new_node, mor);
        return true;
    }
public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
        cds::gc::HP::GuardArray<2> ga;
        auto [parent, curr] = search(ga, key);

        return curr; //узел найден и он не помечен как удаленный, тогда true
    }

    bool insert(const Key& key){
        cds::gc::HP::GuardArray<3> ga;
        bool result = false;
        while(!result) {
            auto [parent, curr] = search(ga, key);

            if(curr!=nullptr) break;

            if(parent != begin_ptr){
                result = insert_new_node(ga, parent, key);
            }else{
                result = insert_header(key);
            }
        }
        return result;
    }

    std::tuple<Node*, Node*, Node*> erase_right_zero(cds::gc::HP::GuardArray<5>& guard_array,
                          Node* parent, Node* curr){
        std::lock_guard lo_lock1(parent->lo_mtx);
        std::lock_guard lo_lock2(curr->lo_mtx);

        Node *next = guard_array.protect(2, curr->next); //right

        std::lock_guard lo_lock3(next->lo_mtx);

        curr->unlink();
        parent->next.store(next, mor);
        next->prev.store(parent, mor);// убрал из списка curr
        parent->r.store(nullptr, mor);

        return {parent, nullptr, nullptr};
    }
    std::tuple<Node*, Node*, Node*> erase_left_zero(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr) {
        while(true) {
            Node *prev = guard_array.protect(2, curr->prev); //left

            std::lock_guard lo_lock1(prev->lo_mtx);
            if (prev->next.load(mor) != curr) continue;
            std::lock_guard lo_lock2(curr->lo_mtx);
            std::lock_guard lo_lock3(parent->lo_mtx);
            curr->unlink();

            parent->prev.store(prev, mor);
            prev->next.store(parent, mor);// убрал из списка curr
            parent->l.store(nullptr, mor);
            break;
        }
        return {parent, nullptr, nullptr};
    }
    std::tuple<Node*, Node*, Node*> erase_right_one(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr, Node* curr_l, Node* curr_r){
        if (curr_l != nullptr) {
            std::lock_guard lock(curr_l->mtx);
            while(true) {
                Node *prev = guard_array.protect(2, curr->prev);
                std::lock_guard lo_lock1(prev->lo_mtx);
                if(prev->next.load(mor) != curr) continue;
                std::lock_guard lo_lock2(curr->lo_mtx);
                Node *next = guard_array.protect(3, curr->next);

                std::lock_guard lo_lock3(next->lo_mtx);

                curr->unlink();

                prev->next.store(next, mor);
                next->prev.store(prev, mor);//удалил из списка
                parent->r.store(curr_l, mor);
                curr_l->p.store(parent, mor);//удалил из дерева
                break;
            }
        } else {
            std::lock_guard lock(curr_r->mtx);
            std::lock_guard lo_lock1(parent->lo_mtx);
            std::lock_guard lo_lock2(curr->lo_mtx);
            Node *next = guard_array.protect(2, curr->next);
            std::lock_guard lo_lock3(next->lo_mtx);

            curr->unlink();

            parent->next.store(next, mor);
            next->prev.store(parent, mor); //удалил из списка

            parent->r.store(curr_r, mor);
            curr_r->p.store(parent, mor);//удалил из дерева
        }
        return {parent, nullptr, nullptr};
    }
    std::tuple<Node*, Node*, Node*> erase_left_one(cds::gc::HP::GuardArray<5>& guard_array,
                        Node* parent, Node* curr, Node* curr_l, Node* curr_r) {
        if (curr_l != nullptr) {
            std::lock_guard lock(curr_l->mtx);
            while(true) {
                Node *prev = guard_array.protect(2, curr->prev);
                std::lock_guard lo_lock1(prev->lo_mtx);
                if (prev->next.load(mor) != curr) continue;
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(parent->lo_mtx);

                curr->unlink();
                parent->prev.store(prev, mor);
                prev->next.store(parent, mor);

                parent->l.store(curr_l, mor);
                curr_l->p.store(parent, mor);
                break;
            }
        } else {
            std::lock_guard lock(curr_r->mtx);
            while(true) {
                Node *prev = guard_array.protect(2, curr->prev);
                std::lock_guard lo_lock1(prev->lo_mtx);
                if(prev->next.load(mor) != curr) continue;
                std::lock_guard lo_lock2(curr->lo_mtx);
                Node *next = guard_array.protect(3, curr->next);
                std::lock_guard lo_lock3(next->lo_mtx);

                curr->unlink();

                next->prev.store(prev, mor);
                prev->next.store(next, mor);//удалил из списка

                parent->l.store(curr_r, mor);
                curr_r->p.store(parent, mor);//удалил из дерева
                break;
            }
        }
        return {parent, nullptr, nullptr};
    }
    template<bool Is_left>
    std::tuple<Node*, Node*, Node*> erase_two(cds::gc::HP::GuardArray<5>& guard_array,
                        Node* parent, Node* curr, Node* curr_l, Node* curr_r) {
        if(curr_l != nullptr) curr_l->mtx.lock();
        while(true) {
            Node *next = guard_array.protect(2, curr->next);
            Node *prev = guard_array.protect(3, curr->prev);
            if (next == curr_r) {
                std::lock_guard lock3(next->mtx);

                if (next->l.load(mor) != nullptr) continue;
                std::lock_guard lo_lock1(prev->lo_mtx);
                if(prev->next.load(mor) != curr) continue;
                std::lock_guard lo_lock2(curr->lo_mtx);
                if(curr->next.load(mor) != next) continue;
                std::lock_guard lo_lock3(next->lo_mtx);

                curr->unlink();

                prev->next.store(next, mor);
                next->prev.store(prev, mor);

                next->l.store(curr_l, mor);
                curr_l->p.store(next, mor);

                if constexpr (Is_left) {
                    parent->l.store(next, mor);
                }else{
                    parent->r.store(next, mor);
                }
                next->p.store(parent, mor);

                if(curr_l != nullptr) curr_l->mtx.unlock();

                return {next, parent, nullptr};
            } else {
                if(curr_r != nullptr) curr_r->mtx.lock();
                Node *next_p = guard_array.protect(4, next->p);
                if (next_p == nullptr) {
                    if(curr_r != nullptr) curr_r->mtx.unlock();
                    continue;
                }
                if(next_p != curr_r) next_p->mtx.lock();

                if (next_p->l.load(mor) != next) {
                    if(curr_r != nullptr) curr_r->mtx.unlock();
                    if(next_p != curr_r) next_p->mtx.unlock();
                    continue;
                }
                std::lock_guard lock4(next->mtx);
                if (next->p.load(mor) != next_p) {
                    if(curr_r != nullptr) curr_r->mtx.unlock();
                    if(next_p != curr_r) next_p->mtx.unlock();
                    continue;
                }
                if (next->l.load(mor) != nullptr) {
                    if(curr_r != nullptr) curr_r->mtx.unlock();
                    if(next_p != curr_r) next_p->mtx.unlock();
                    continue;
                }

                std::lock_guard lo_lock1(prev->lo_mtx);
                if(prev->next.load(mor) != curr) {
                    if(curr_r != nullptr) curr_r->mtx.unlock();
                    if(next_p != curr_r) next_p->mtx.unlock();
                    continue;
                }
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);

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

                if constexpr (Is_left) {
                    parent->l.store(next, mor);
                }else{
                    parent->r.store(next, mor);
                }
                next->p.store(parent, mor);
                next->height.store(curr->height.load());

                if(curr_l != nullptr) curr_l->mtx.unlock();
                if(curr_r != nullptr) curr_r->mtx.unlock();
                if(next_p != curr_r) next_p->mtx.unlock();

                return {next_p, next, parent};
            }
        }
    }
    bool erase_right(cds::gc::HP::GuardArray<5>& guard_array,
                     Node* parent, Node* curr){
        constexpr bool Is_right{false};
        std::tuple<Node*, Node*, Node*> to_fix = {nullptr, nullptr, nullptr};
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->r.load(mor) != curr) return false;

            std::lock_guard lock2(curr->mtx);
            Node *curr_l = curr->l.load(mor);
            Node *curr_r = curr->r.load(mor);
            int n_children = (curr_l != nullptr)
                             + (curr_r != nullptr);
            if(n_children == 0) to_fix = erase_right_zero(guard_array, parent, curr);
            else if(n_children == 1) to_fix = erase_right_one(guard_array, parent, curr, curr_l, curr_r);
            else if(n_children == 2) to_fix = erase_two<Is_right>(guard_array, parent, curr, curr_l, curr_r);
            guard_array.assign(2, std::get<0>(to_fix));
            guard_array.assign(3, std::get<1>(to_fix));
            guard_array.assign(4, std::get<2>(to_fix));

        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        fix(guard_array, std::get<0>(to_fix));
        fix(guard_array, std::get<1>(to_fix));
        fix(guard_array, std::get<2>(to_fix));
        return true;
    }
    bool erase_left(cds::gc::HP::GuardArray<5>& guard_array,
                    Node* parent, Node* curr){
        constexpr bool Is_left{true};
        std::tuple<Node*, Node* ,Node*> to_fix;
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->l.load(mor) != curr) return false;

            std::lock_guard lock2(curr->mtx);

            Node *curr_l = curr->l.load(mor);
            Node *curr_r = curr->r.load(mor);
            int n_children = (curr_l != nullptr)
                             + (curr_r != nullptr);

            if(n_children == 0) to_fix = erase_left_zero(guard_array, parent, curr);
            else if(n_children == 1) to_fix = erase_left_one(guard_array, parent, curr, curr_l, curr_r);
            else if(n_children == 2) to_fix = erase_two<Is_left>(guard_array, parent, curr, curr_l, curr_r);
            guard_array.assign(2, std::get<0>(to_fix));
            guard_array.assign(3, std::get<1>(to_fix));
            guard_array.assign(4, std::get<2>(to_fix));
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        fix(guard_array, std::get<0>(to_fix));
        fix(guard_array, std::get<1>(to_fix));
        fix(guard_array, std::get<2>(to_fix));
        return true;
    }
    bool erase_header(cds::gc::HP::GuardArray<5>& guard_array, Node* curr){
        std::tuple<Node*, Node*, Node*> to_fix;
    {
        constexpr bool Is_right{false};
        std::lock_guard lock1(begin_ptr->mtx);
        if(begin_ptr->r.load(mor) != curr) return false;
        std::lock_guard lock2(curr->mtx);
        Node *curr_l = curr->l.load(mor);//deleted?
        Node *curr_r = curr->r.load(mor);
        int n_children = (curr_l != nullptr)
                         + (curr_r != nullptr);
        if(n_children == 0) to_fix = erase_right_zero(guard_array, begin_ptr, curr);
        else if(n_children == 1) to_fix = erase_right_one(guard_array, begin_ptr, curr, curr_l, curr_r);
        else if(n_children == 2) to_fix = erase_two<Is_right>(guard_array, begin_ptr, curr, curr_l, curr_r);
        guard_array.assign(2, std::get<0>(to_fix));
        guard_array.assign(3, std::get<1>(to_fix));
        guard_array.assign(3, std::get<2>(to_fix));

    }
    cds::gc::HP::retire<NodeDisposer>(curr);
    fix(guard_array, std::get<0>(to_fix));
    fix(guard_array, std::get<1>(to_fix));
    fix(guard_array, std::get<2>(to_fix));
    return true;
    }
public:
    bool erase(const Key& key){

        cds::gc::HP::GuardArray<5> ga;
        bool result = false;
        while (!result) {
            auto [parent, curr] = search(ga, key);
            if (!curr) break;
            if(parent != begin_ptr){
                if(comp(curr->key, parent->key)){
                    result = erase_left(ga, parent, curr);
                }else{
                    result = erase_right(ga, parent, curr);
                }
            }else{
                result = erase_header(ga,curr);
                result = true;
            }

        }
        return result;
    }
//    int traverse_all_reverse(){
//        Node* begin = begin_node.load();
//        Node* curr = end_node.load()->prev;
//        int c = 0;
//        int co = 0;
//        while(curr != begin){
//            co++;
//            Node* next = curr->prev.load();
//            if(next == begin){
//                break;
//            }
//            if(curr->key <= next->key){
//                c++;
//                throw "error";
//            }
//            curr = next;
//        }
//        if(s==0) check_heights();
//
//        if(co != s || c!=0){
//            return 1;
//        }
//        return 0;
//    }
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
        traverse_all(begin_ptr->r.load(mor));
        if(last_node->next.load() != end_ptr){
            throw "145";
        }
    }
    int false_balance = 0;
    void check_heights(Node* curr){
        if(curr==nullptr){
            return;
        }
        check_heights(curr->l.load());
        check_heights(curr->r.load());
        curr->recalculate_height();
        if(curr->get_balance_factor() >1 || curr->get_balance_factor() < -1){
            ++false_balance;
            std::cout << std::endl;
            std::cout << curr->key << ' ';
        if(curr->l.load()!=nullptr){
            std::cout << curr->l.load()->last_balance << ' ';
        }
            if(curr->r.load()!=nullptr){
                std::cout << curr->r.load()->last_balance << ' ';
            }
         std::cout << curr->last_balance << " imposter??" << std::endl;
        }
    }
    void check_heights(){
        false_balance = 0;
        check_heights(begin_ptr->r.load());
    }

    enum class Direction{Left, Right};
//    template<Direction DIR>
    Node* fix_height(Node* curr, Node* parent){
        std::lock_guard lock1(parent->mtx);
        if(parent->l.load(mor) != curr && parent->r.load(mor) != curr) return curr;
        std::lock_guard lock(curr->mtx);
        auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);

        if(curr->isUnlinked()) return nullptr;
        if(balance_factor< -1 || balance_factor > 1){ //пока брали блокировку, в приоритете стало произвести поворот
            return curr;                              //прервать изменение высоты и повторить балансировку для curr
        }

        if(new_curr_height == curr->height.load(mor)){//пока брали блокировку, кто-то поправил высоту
            return nullptr;                 //закончить fix(теперь fix это не забота этого потока)
        }
        curr->height.store(new_curr_height, mor);
        return parent; // теперь надо чинить родителя
    }
    template<Direction DIR>
    void internal_right_rotate(Node* curr, Node* parent, Node* curr_l){
        Node* curr_lr = curr_l->r.load();
        //передаем curr правого ребенка от curr_l_child
        curr->l.store(curr_lr);
        if(curr_lr!=nullptr) {
            curr_lr->p.store(curr);
        }
        //curr опускается ниже и делается правым ребенком curr_l_child
        curr->p.store(curr_l);
        curr_l->r.store(curr);

        curr_l->p.store(parent);
        if constexpr (DIR == Direction::Left) {
            parent->l.store(curr_l);
        } else {
            parent->r.store(curr_l);
        }
    }

    template<Direction DIR>
    void internal_left_rotate(Node* curr, Node* parent, Node* curr_r){
        //теперь parent указывает на правого ребенка curr
        Node* curr_rl = curr_r->l.load();

        //передаем curr левого ребенка от curr_r_child
        curr->r.store(curr_rl);
        if(curr_rl!=nullptr) {//not need mb
            curr_rl->p.store(curr);
        }
        //curr опускается ниже и делается правым ребенком curr_l_child
        curr->p.store(curr_r);
        curr_r->l.store(curr);

        curr_r->p.store(parent);
        if constexpr (DIR == Direction::Left) {
            parent->l.store(curr_r);
        } else {
            parent->r.store(curr_r);
        }

    }

    template<Direction DIR>
    Node* big_right_rotate(Node* curr, Node* parent, Node* curr_l,
                           Node* curr_lr){

       int bf_child;

        internal_left_rotate<Direction::Left>(curr_l, curr, curr_lr);
        internal_right_rotate<DIR>(curr, parent, curr_lr);

        curr->recalculate_height();
        curr_l->recalculate_height();
        curr_lr->recalculate_height();

        bf_child = curr->get_balance_factor();

        if(bf_child<-1 || bf_child>1) {
            return curr;
        }
        bf_child = curr_lr->get_balance_factor();

        if(bf_child<-1 || bf_child>1) {
            return curr_lr;
        }

        return parent;
    }
    template<Direction DIR>
    Node* big_left_rotate(Node* curr, Node* parent, Node* curr_r,
                        Node* curr_rl){
        int bf_child;
        internal_right_rotate<Direction::Right>(curr_r, curr, curr_rl);
        internal_left_rotate<DIR>(curr, parent, curr_rl);


        curr->recalculate_height();
        curr_r->recalculate_height();
        curr_rl->recalculate_height();

        bf_child = curr->get_balance_factor();
        if(bf_child<-1 || bf_child>1){
            return curr;
        }

        bf_child = curr_rl->get_balance_factor();
        if(bf_child<-1 || bf_child>1) {
            return curr_rl;
        }

        return parent;
    }
    template<Direction DIR>
    Node* small_right_rotate(Node* curr, Node* parent, Node* curr_l){

        internal_right_rotate<DIR>(curr, parent, curr_l);
        curr->recalculate_height();
        curr_l->recalculate_height();
        int bf_child;
        bf_child = curr->get_balance_factor();

        if(bf_child<-1 || bf_child>1) {
            return curr;
        }


        bf_child = curr_l->get_balance_factor();
        if (bf_child < -1 || bf_child > 1){
            return curr_l;
        }
        return parent;
    }

    template<Direction DIR>
    Node* right_rotate(Node* curr, Node* parent, Node* curr_l){
        //тут уже должна быть взята блокировка на curr и parent и curr_l_child

        int l_balance_factor = curr_l->get_balance_factor();
        if(l_balance_factor >= 0){
            return small_right_rotate<DIR>(curr, parent, curr_l); //curr и parent уже взяты в lock
        }else{
            Node* curr_lr = curr_l->r.load();
            std::lock_guard lock(curr_lr->mtx);
            int b = Node::get_height(curr_l->l.load()) -
                    Node::get_height(curr_lr->l.load());
            if(b <-1 || b >1) {
                return left_rotate<Direction::Left>(curr_l, curr, curr_lr);
            }
            return big_right_rotate<DIR>(curr, parent, curr_l, curr_lr);
        }
    }
    template<Direction DIR>
    Node* small_left_rotate(Node* curr, Node* parent, Node* curr_r){
        internal_left_rotate<DIR>(curr, parent, curr_r);
        curr->recalculate_height();
        curr_r->recalculate_height();

        int bf_child;
        bf_child = curr->get_balance_factor();
        if(bf_child<-1 || bf_child>1) {
            return curr;
        }
        bf_child = curr_r->get_balance_factor();

        if(bf_child<-1 || bf_child>1) {
            return curr_r;
        }
        return parent;
    }
    template<Direction DIR>
    Node* left_rotate(Node* curr, Node* parent, Node* curr_r){
        //тут уже должна быть взята блокировка на curr и parent

        int r_balance_factor = curr_r->get_balance_factor();
        if(r_balance_factor <= 0){
            return small_left_rotate<DIR>(curr, parent, curr_r); //curr и parent уже взяты в lock
        }else{
            Node* curr_rl = curr_r->l.load();
            std::lock_guard lock(curr_rl->mtx);
            int b = Node::get_height(curr_r->r.load()) -
                    Node::get_height(curr_rl->r.load());
            if(b<-1||b>1) {
                return right_rotate<Direction::Right>(curr_r, curr, curr_rl);
            }
            return big_left_rotate<DIR>(curr, parent, curr_r, curr_rl);
        }
    }
    template<Direction DIR>
    Node* fix_balance(Node* curr, Node* parent){
        std::lock_guard lock1(parent->mtx);
        if constexpr (DIR == Direction::Left) {
            if (parent->l.load(mor) != curr) return curr;
        }else{
            if (parent->r.load(mor) != curr) return curr;
        }
        std::lock_guard lock2(curr->mtx);
        if(curr->isUnlinked()) return nullptr;
        auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);
        if(balance_factor > 1){
            Node* curr_l = curr->l.load();
            std::lock_guard lock(curr_l->mtx);
            return right_rotate<DIR>(curr, parent, curr_l);
        }else if(balance_factor < -1){
            Node* curr_r = curr->r.load();
            std::lock_guard lock(curr_r->mtx);
            return left_rotate<DIR>(curr, parent, curr_r);
        }else{ // пока брали блокировку, кто-то поправил баланс, возможно надо поправить высоту(раз уже взяли блокировку)
            if(curr->height.load(mor) == new_curr_height){ // высоту тоже кто-то поправил
                return nullptr;                  // закончить fix(теперь fix это не забота этого потока)
            }
            curr->height.store(new_curr_height,mor);
            return parent; // теперь надо чинить родителя
        }
    }
    std::tuple<int, int, int, int> take_node_params(Node* curr){
        Node* l = curr->l.load(mor);
        Node* r = curr->r.load(mor);
        int l_height = l == nullptr? 0 : l->height.load(mor);
        int r_height = r == nullptr? 0 : r->height.load(mor);
        int new_curr_height = 1 + std::max(l_height, r_height);
        int balance_factor = l_height - r_height;
        return {l_height, r_height, new_curr_height, balance_factor};
    }

    template<int N>
    void fix(cds::gc::HP::GuardArray<N>& ga, Node* curr){
        int step =0;
        ga.assign(step, curr);
        step++;

        while(curr != nullptr && curr != begin_ptr){

            auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);
            Node* parent = ga.protect(step%2, curr->p);
            step++;

            if(parent == nullptr) return;
            if(balance_factor>=-1 && balance_factor <=1){//с высотами детей все в порядке
                if(new_curr_height == curr->height.load()){
//                    кто-то все починил
                    return;
                }
                curr = fix_height(curr, parent);
            }else{
                if(comp(curr->key, parent->key)) {
                    curr = fix_balance<Direction::Left>(curr, parent);
                }else{
                    curr = fix_balance<Direction::Right>(curr, parent);
                }
            }
        }
    }
public:
    void print(Node* curr, int dep, char ch){
        if(curr == nullptr){
            return;
        }
        print(curr->l.load(), dep + 1, '/');
        std::cout.width(dep * 5);
        std::cout << ch << "(" << curr->key << ',' <<curr->get_balance_factor() << ") \n";
        print(curr->r.load(), dep + 1, '\\');
    }
    void print(){
        print(begin_ptr->r.load(), 0, '=');
    }
    void insert_node_params(std::vector<int>& image, Node* curr){
        image.push_back(curr->key.load());
        int l_key = curr->l.load() ? curr->l.load()->key : -3;
        int r_key = curr->r.load() ? curr->r.load()->key : -3;
        int p_key = curr->p.load() ? curr->p.load()->key : -3;
        image.push_back(l_key);
        image.push_back(r_key);
        image.push_back(p_key);
    }
    void get_tree_image(std::vector<int>& image, Node* curr){
        Node* l = curr->l.load(), r = curr->r.load();
        if(l) { image.push_back(-1);  get_tree_image(image, l);}
        insert_node_params(image, curr);
        if(r){ image.push_back(-2); get_tree_image(image, r);}
    }
    std::vector<int> get_tree_image(){
        std::vector<int> image;
        get_tree_image(image, begin_ptr->r.load());
        return std::move(image);
    }
};


#endif //BACHELOR_CONCURRENT_AVL_LO_H
