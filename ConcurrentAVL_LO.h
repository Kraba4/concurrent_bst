//
// Created by HONOR on 01.06.2023.
//

#ifndef BACHELOR_CONCURRENTAVL_LO_H
#define BACHELOR_CONCURRENTAVL_LO_H
#include <functional>
#include <memory>
#include <mutex>
#include <cds/sync/spinlock.h>
#include <cds/gc/hp.h>

template <typename Key, typename Compare = std::less<Key>, typename Alloc = std::allocator<Key>>
class ConcurrentAVL_LO{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key value;
        int height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
        std::atomic<Node*> p;
        std::atomic<Node*> next;
        std::atomic<Node*> prev;
        Node* last_prev = nullptr;
        Node* last_next = nullptr;
        cds::sync::spin_lock<cds::backoff::LockDefault> mtx;
        cds::sync::spin_lock<cds::backoff::LockDefault> lo_mtx;
        /* метка того, что вершина удалена(недостижима в дереве). Нужно потому, что пока один поток
         * нашел вершину и собирается начать работать с ней, другой поток может удалить ее из дерева*/
        bool deleted;
        int desc;
        void unlink(){
            prev.store(nullptr);
            next.store(nullptr);
            l.store(nullptr);
            r.store(nullptr);
            p.store(nullptr);
            deleted = true;
        }
        Node(const Key& value, Node* parent): l(), r(), p(parent),
                                value(value), height(0), deleted(false){
        }
        Node(){}
        ~Node(){
//            std::cout << "destroy " << value << std::endl;
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

public:
    ConcurrentAVL_LO(){
        Node* begin = Node::createNode(alloc, -100000, nullptr);
        Node* end =Node::createNode(alloc, 1000000000, nullptr);
        begin_ptr = begin;
        end_ptr = end;
        begin->prev.store(nullptr);
        begin->next.store(end);
        end->prev.store(begin);
        end->next.store(nullptr);
        begin_node.store(begin);
        end_node.store(end);
    }

    class iterator {
    private:
        cds::gc::HP::Guard guarder;
        Node* node;
        std::atomic<Node*>* header;
    public:
//        operator basenode* () { return node; }
        friend class ConcurrentAVL_LO;
        // Надо объявить вложенные типы итератора
//        typedef std::bidirectional_iterator_tag iterator_category;
//        typedef T value_type;
//        typedef int difference_type;
//        typedef T& reference;
//        typedef T* pointer;
//        iterator(basenode* nd) : node(nd) {}

        iterator(std::atomic<Node*>* header, std::atomic<Node*>* node)
        : header(header), node(guarder.protect(*node)){}

        iterator(std::atomic<Node*>* header){

        }
        iterator(const iterator& other){
            guarder.copy(other.guarder);
            node = other.node;
            header = other.header;
        }
        iterator operator++() {
            Key value = node->value;
            Node* new_node = guarder.protect(node->next);

            if(new_node == nullptr) {
                cds::gc::HP::GuardArray<2> ga;
                while (new_node == nullptr || new_node->value <= value) {
                    new_node = simple_search(ga, *header, value);
                    if (new_node != nullptr) {
                        if (new_node->value <= value) {
                            new_node = guarder.protect(new_node->next);
                        } else {
                            guarder.assign(new_node);
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
            Key value = node->value;
            Node* new_node = guarder.protect(node->prev);

            if(new_node == nullptr) {
                cds::gc::HP::GuardArray<2> ga;
                while (new_node == nullptr || new_node->value >= value) {
                    new_node = simple_search(ga, *header, value);
                    if (new_node != nullptr) {
                        if (new_node->value >= value) {
                            new_node = guarder.protect(new_node->prev);
                        } else {
                            guarder.assign(new_node);
                        }
                    }
                }
            }
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
            std::cout << node->value << std::endl;
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
            if (equiv(curr->value, key)) {
                break;
            }
            step++;
            parent = curr;
            if (comp(key, curr->value)) {
                curr = guard_array.protect(step % 2, curr->l);
            } else {
                curr = guard_array.protect(step % 2, curr->r);
            }
        }
        if(curr) return curr;
        return parent;
    }

        std::tuple<Node*, Node*>
    search(cds::gc::HP::GuardArray<5>& guard_array,
           const std::atomic<Node*>& root,
           const Key& key) const{
        Node* parent = nullptr;
        int step=0;
        Node* curr = guard_array.protect(step, root);

        while(curr){
            if(equiv(curr->value,key)){
                break;
            }
            step++;
            parent = curr;
            if(comp(key,curr->value)){
                curr = guard_array.protect(step%2,curr->l);
            }else{
                curr = guard_array.protect(step%2,curr->r);
            }
        }

        if(curr == nullptr) {
            step++;
            if (key < parent->value) {
                Node* new_parent = begin_ptr;
                while (key < parent->value) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->prev);
                    if(new_parent == begin_ptr) break;
                    if(new_parent == nullptr) return {parent, curr};
                    parent = new_parent;
                }
                if (new_parent != begin_ptr && parent->r.load() != nullptr) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->next);
                    if(new_parent == nullptr) return {parent, curr};
                    if(new_parent != end_ptr){
                        parent = new_parent;
                    }
                }
            } else {
                Node* new_parent = end_ptr;
                while (key > parent->value) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->next);
                    if(new_parent == end_ptr) break;
                    if(new_parent == nullptr) return {parent, curr};
                    parent = new_parent;
                }
                if (new_parent != end_ptr && parent->l.load() != nullptr) {
                    step++;
                    new_parent = guard_array.protect(step%2, parent->prev);
                    if(new_parent == nullptr) return {parent, curr};
                    if(new_parent != begin_ptr){
                        parent = new_parent;
                    }
                }
            }
            if(parent->value == key){
                curr = parent;
                parent = guard_array.protect(step%2, curr->p);
            }
        }

        return {parent, curr};
    }
private:
    bool insert_left_child(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, const Key& key){
        {
            while(true) {
                    std::lock_guard lock1(parent->mtx);
                    Node *prev = guard_array.protect(2, parent->prev);
                    if (parent->deleted) return false;
                    if (prev->value > key) return false;
                    if (prev->value == key) return true;
                    std::lock_guard lock2(prev->lo_mtx);
                    if(prev->deleted) continue;

                    std::lock_guard lock3(parent->lo_mtx);
                    if(parent->l.load() !=nullptr) return false;
                    if(parent->prev.load() != prev || prev->next.load() != parent) continue;

                    Node* new_node = Node::createNode(alloc, key, parent);
                    new_node->prev.store(prev); new_node->next.store(parent);
                    prev->next.store(new_node); parent->prev.store(new_node);
                    parent->l.store(new_node);
                    break;
                }
        }
//        fix(parent);
        return true;
    }
    bool insert_right_child(cds::gc::HP::GuardArray<5>& guard_array,
                            Node* parent, const Key& key) {
        std::lock_guard lock1(parent->mtx);
        if(parent->deleted){
            return false;
        }
        if(parent->r.load() != nullptr){
            return false;
        }
        Node *next = guard_array.protect(2, parent->next);
        if(next->value < key) return false;
        if(next->value == key) return true;
        std::lock_guard lock2(parent->lo_mtx);
        std::lock_guard lock3(next->lo_mtx);
        if(parent->next.load() != next) return false;
        Node* new_node = Node::createNode(alloc, key, parent);
        new_node->prev.store(parent); new_node->next.store(next);
        parent->next.store(new_node); next->prev.store(new_node);
        parent->r.store(new_node, std::memory_order_seq_cst);
        return true;
    }
    bool insert_new_node(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, const Key& key) {
        if (comp(key, parent->value)) {
            return insert_left_child(guard_array, parent, key);
        }else {
            return insert_right_child(guard_array, parent, key);
        }
    }
public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
        if(header.load(std::memory_order_seq_cst) == nullptr){
            return false;
        }
        cds::gc::HP::GuardArray<5> ga;
        auto [parent, curr] = search(ga, header, key);
        return curr; //узел найден и он не помечен как удаленный, тогда true
    }

    void insert(const Key& key){
        cds::gc::HP::GuardArray<5> ga;
        while(true) {
            if (header.load(std::memory_order_seq_cst) == nullptr) {
                std::lock_guard lock(null_header_mtx);
                if(header.load(std::memory_order_seq_cst) != nullptr){ //пока брали блокировку, кто-то занял header
                    continue;
                }
                Node* new_node = Node::createNode(alloc, key, nullptr);
                Node* begin = begin_node.load();
                Node* end = end_node.load();
                new_node->prev.store(begin);
                new_node->next.store(end);
                begin->next.store(new_node);
                end->prev.store(new_node);
                header.store(new_node, std::memory_order_seq_cst);
                break;
            }

            auto [parent, curr] = search(ga, header, key);
            if(curr!=nullptr){
                break;
            }
            bool result = insert_new_node(ga, parent, key);
            if(result){
                break;
            }else{
                ++retries;
            }
        }
    }

    bool erase_right_zero(cds::gc::HP::GuardArray<5>& guard_array,
                          Node* parent, Node* curr){
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->deleted) return false;
            if (parent->r.load() != curr) return false; //right

            std::lock_guard lock2(curr->mtx);
            if (curr->deleted) return true;//сама удалилась
            if (curr->p.load() != parent) return false; //not need mb
            int n_children = (curr->l.load(std::memory_order_seq_cst) != nullptr)
                             + (curr->r.load(std::memory_order_seq_cst) != nullptr);
            if (n_children != 0) return false;

            Node *next = guard_array.protect(2, curr->next); //right

            std::lock_guard lo_lock1(parent->lo_mtx);
            if (parent->next.load() != curr) return false; //not need
            std::lock_guard lo_lock2(curr->lo_mtx);
            if (curr->prev.load() != parent) return false; // not need
            std::lock_guard lo_lock3(next->lo_mtx);
            if (next->deleted) return false;
            if (next->prev.load() != curr) return false;
            if (curr->next.load() != next) return false;//not need?
            assert(parent->value < curr->value);
            assert(curr->value < next->value);
            curr->unlink();
            curr->desc = 0;
            assert(next != curr);
            parent->next.store(next);
            next->prev.store(parent);// убрал из списка curr
            parent->r.store(nullptr);
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
    bool erase_left_zero(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr) {
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->deleted) return false;
            if (parent->l.load() != curr) return false; //left

            std::lock_guard lock2(curr->mtx);
            if (curr->deleted) return true;//удалился сам
            if (curr->p.load() != parent) return false; //not need mb
            int n_children = (curr->l.load(std::memory_order_seq_cst) != nullptr)
                             + (curr->r.load(std::memory_order_seq_cst) != nullptr);
            if (n_children != 0) return false;

            Node *prev = guard_array.protect(2, curr->prev); //left

            std::lock_guard lo_lock1(prev->lo_mtx);
            if (prev->deleted) return false;
            if (prev->next.load() != curr) return false;
            std::lock_guard lo_lock2(curr->lo_mtx);
            if (curr->next.load() != parent) return false; // not need
            if (curr->prev.load() != prev) return false;//not
            std::lock_guard lo_lock3(parent->lo_mtx);
            if (parent->prev.load() != curr) return false; //not need
            assert(prev->value < curr->value);
            assert(curr->value < parent->value);
            curr->unlink();
            curr->desc = 1;
            curr->last_prev = prev;
            curr->last_next = parent;
            parent->prev.store(prev);
            assert(parent != curr);
            prev->next.store(parent);// убрал из списка curr
            parent->l.store(nullptr);
        }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
    bool erase_right_one(cds::gc::HP::GuardArray<5>& guard_array,
                         Node* parent, Node* curr){
        {
            std::lock_guard lock1(parent->mtx);
            if (parent->deleted) return false;
            if (parent->r.load() != curr) return false; //right

            std::lock_guard lock2(curr->mtx);
            if (curr->deleted) return true;//удалился сам
            if (curr->p.load() != parent) return false;//not
            Node *curr_l = curr->l.load();
            Node *curr_r = curr->r.load();
            if (curr_l != nullptr && curr_r == nullptr) {
                Node *next = guard_array.protect(2, curr->next);
                Node *prev = guard_array.protect(3, curr->prev);

                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->deleted) return false;
                if (prev->deleted) return false;
                if (curr->prev.load() != prev) return false;
                if (next->prev.load() != curr) return false;
                if (curr->next.load() != next) return false; //not
                assert(prev->value < curr->value);
                assert(curr->value < next->value);
                curr->unlink();
                curr->desc = 2;
                curr->last_prev = prev;
                curr->last_next = next;
                assert(curr != next);
                prev->next.store(next);
                next->prev.store(prev);//удалил из списка
                assert(prev->next.load() == next);
                parent->r.store(curr_l);
                curr_l->p.store(parent);//удалил из дерева

            } else if (curr_l == nullptr && curr_r != nullptr) {
                Node *next = guard_array.protect(2, curr->next);
                std::lock_guard lo_lock1(parent->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->deleted) return false;
                if (curr->next.load() != next) return false;
                if (next->prev.load() != curr) return false;//not

                assert(parent->value < curr->value);
                assert(curr->value < next->value);
                curr->unlink();
                curr->desc = 3;
                curr->last_prev = parent;
                assert(curr != next);
                parent->next.store(next);
                next->prev.store(parent); //удалил из списка

                parent->r.store(curr_r);
                curr_r->p.store(parent);//удалил из дерева
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
            if (parent->deleted) return false;
            if (parent->l.load() != curr) return false; //left

            std::lock_guard lock2(curr->mtx);
            if (curr->deleted) return true;//удалился сам
            if (curr->p.load() != parent) return false;//not
            Node *curr_l = curr->l.load();
            Node *curr_r = curr->r.load();
            if (curr_l != nullptr && curr_r == nullptr) {
                Node *prev = guard_array.protect(2, curr->prev);
                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(parent->lo_mtx);
                if (prev->deleted) return false;
                if (curr->prev.load() != prev) return false;
                if (prev->next.load() != curr) return false; //not

                assert(prev->value < curr->value);
                assert(curr->value < parent->value);
                curr->unlink();
                curr->desc = 4;
                curr->last_prev = prev;
                curr->last_next = parent;
                assert(parent != curr);
                assert(prev != curr);
                parent->prev.store(prev);
                prev->next.store(parent);

                parent->l.store(curr_l);
                curr_l->p.store(parent);
            } else if (curr_l == nullptr && curr_r != nullptr) {
                Node *prev = guard_array.protect(2, curr->prev);
                Node *next = guard_array.protect(3, curr->next);

                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->deleted) return false;
                if (prev->deleted) return false;
                if (curr->prev.load() != prev) return false;
                if (next->prev.load() != curr) return false;
                if (curr->next.load() != next) return false; //not

                assert(prev->value < curr->value);
                assert(curr->value < next->value);
                curr->unlink();
                curr->desc = 5;
                next->prev.store(prev);
                assert(next != curr);
                prev->next.store(next);//удалил из списка

                parent->l.store(curr_r);
                curr_r->p.store(parent);//удалил из дерева
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
            if (parent->deleted) return false;
            if (parent->r.load() != curr) return false; //right

            std::lock_guard lock2(curr->mtx);
            if (curr->deleted) return true;//удалился сам
            if (curr->p.load() != parent) return false;//not
            Node *curr_l = curr->l.load();//deleted?
            Node *curr_r = curr->r.load();
            int n_children = (curr_l != nullptr)
                             + (curr_r != nullptr);
            if (n_children != 2) return false;

            Node *next = guard_array.protect(2, curr->next);
            Node *prev = guard_array.protect(3, curr->prev);
            if (next == curr_r) {
                std::unique_lock lock3(next->mtx);
                if (next == end_ptr) lock3.unlock();

                if (next->l.load() != nullptr) return false;
                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->deleted) return false;
                if (prev->deleted) return false;
                if (curr->next.load() != next) return false;
                if (curr->prev.load() != prev) return false;

                curr->unlink();

                prev->next.store(next);
                next->prev.store(prev);

                next->l.store(curr_l);
                curr_l->p.store(next);

                parent->r.store(next);
                next->p.store(parent);
            } else {
                Node *next_p = guard_array.protect(4, next->p);
                if (next_p == nullptr) return false;
                std::lock_guard lock3(next_p->mtx);
                if (next_p->deleted) return false;
                if (next_p->l.load() != next) return false;
                std::lock_guard lock4(next->mtx);
                if (next->p.load() != next_p) return false;
                if (next->l.load() != nullptr) return false;

                std::lock_guard lo_lock1(prev->lo_mtx);
                std::lock_guard lo_lock2(curr->lo_mtx);
                std::lock_guard lo_lock3(next->lo_mtx);
                if (next->deleted) return false;
                if (prev->deleted) return false;
                if (curr->next.load() != next) return false;
                if (curr->prev.load() != prev) return false;

                curr->unlink();

                prev->next.store(next);
                next->prev.store(prev);

                Node *next_r = next->r.load();
                next_p->l.store(next_r);
                if (next_r) next_r->p.store(next_p);

                next->l.store(curr_l);
                next->r.store(curr_r);
                if (curr_l) curr_l->p.store(next);
                if (curr_r) curr_r->p.store(next);

                parent->r.store(next);
                next->p.store(parent);
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
        if (parent->deleted) return false;
        if (parent->l.load() != curr) return false; //right

        std::lock_guard lock2(curr->mtx);
        if (curr->deleted) return true;//удалился сам
        if (curr->p.load() != parent) return false;//not
        Node *curr_l = curr->l.load();//deleted?
        Node *curr_r = curr->r.load();
        int n_children = (curr_l != nullptr)
                         + (curr_r != nullptr);
        if (n_children != 2) return false;

        Node *next = guard_array.protect(2, curr->next);
        Node *prev = guard_array.protect(3, curr->prev);
        if (next == curr_r) {
//            std::lock_guard lock3(next->mtx);
            std::unique_lock lock3(next->mtx);
            if (next == end_ptr) lock3.unlock();

            if (next->l.load() != nullptr) return false;
            std::lock_guard lo_lock1(prev->lo_mtx);
            std::lock_guard lo_lock2(curr->lo_mtx);
            std::lock_guard lo_lock3(next->lo_mtx);
            if (next->deleted) return false;
            if (prev->deleted) return false;
            if (curr->next.load() != next) return false;
            if (curr->prev.load() != prev) return false;

            curr->unlink();

            prev->next.store(next);
            next->prev.store(prev);

            next->l.store(curr_l);
            curr_l->p.store(next);

            parent->l.store(next);
            next->p.store(parent);
        } else {
            Node *next_p = guard_array.protect(4, next->p);
            if (next_p == nullptr || next_p->deleted) return false;
            std::lock_guard lock3(next_p->mtx);
            if (next_p->deleted) return false;
            if (next_p->l.load() != next) return false;
            std::lock_guard lock4(next->mtx);
            if (next->p.load() != next_p) return false;
            if (next->l.load() != nullptr) return false;

            std::lock_guard lo_lock1(prev->lo_mtx);
            std::lock_guard lo_lock2(curr->lo_mtx);
            std::lock_guard lo_lock3(next->lo_mtx);
            if (next->deleted) return false;
            if (prev->deleted) return false;
            if (curr->next.load() != next) return false;
            if (curr->prev.load() != prev) return false;

            curr->unlink();

            prev->next.store(next);
            next->prev.store(prev);

            Node *next_r = next->r.load();
            next_p->l.store(next_r);
            if (next_r) { next_r->p.store(next_p); }

            next->l.store(curr_l);
            next->r.store(curr_r);
            if (curr_l) curr_l->p.store(next);
            if (curr_r) curr_r->p.store(next);

            parent->l.store(next);
            next->p.store(parent);
        }
    }
        cds::gc::HP::retire<NodeDisposer>(curr);
        return true;
    }
public:
    std::atomic<int> retries = 0;
    bool erase(const Key& key){
        cds::gc::HP::GuardArray<5> ga;
        while (true) {
            auto [parent, curr] = search(ga,header, key);
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
            int n_children = (curr->l.load(std::memory_order_seq_cst) != nullptr)
                             + (curr->r.load(std::memory_order_seq_cst) != nullptr);
            bool result;
            if(comp(curr->value, parent->value)){
                if(n_children == 0) result = erase_left_zero(ga, parent, curr);else
                if(n_children == 1) result = erase_left_one(ga, parent, curr);else
                if(n_children == 2) result = erase_left_two(ga, parent, curr);
            }else{
                if(n_children == 0) result = erase_right_zero(ga, parent, curr);else
                if(n_children == 1) result = erase_right_one(ga, parent, curr);else
                if(n_children == 2) result = erase_right_two(ga, parent, curr);
            }
            if(result){
                break;
            }else{
                ++retries;
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
            if(curr->value <= next->value){
                c++;
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
    int traverse_all(){
        Node* end = end_node.load();
        Node* curr = begin_node.load()->next;
        int c = 0;
        int co = 0;
        while(curr != end){
            co++;
            Node* next = curr->next.load();
            if(next == end){
                break;
            }
            if(curr->value >= next->value){
                c++;
            }
            curr = next;
        }
        if(s==0) check_heights();
//        std::cout << "order = " << c << " size " << (co == s) << " " << co << " " << s << std::endl;
        if(co != s || c != 0){
            return 1;
        }
        return 0;
    }
    int s = 0;
    int last = -1000000;
    int er = 0;
    void check_heights(Node* curr){
        if(curr==nullptr){
            return;
        }
        check_heights(curr->l.load());
        if(curr->value <= last){
            er++;
        }
        last = curr->value;
        check_heights(curr->r.load());
//        curr->recalculate_height();

        s++;
    }
    void check_heights(){
        check_heights(header.load());
    }
};
#endif //BACHELOR_CONCURRENTAVL_LO_H
