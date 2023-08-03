//
// Created by HONOR on 17.05.2023.
//

#ifndef BACHELOR_CONCURRENTAVL_H
#define BACHELOR_CONCURRENTAVL_H


#include <functional>
#include <memory>
#include <mutex>
#include <cds/sync/spinlock.h>
#include <cds/gc/hp.h>


template<typename T>
using guarded_ptr = cds::gc::HP::guarded_ptr<T>;

using guard = cds::gc::HP::Guard;
#define mmo std::memory_order_seq_cst

template <typename Key, typename Compare = std::less<Key>, typename Alloc = std::allocator<Key>>
class ConcurrentAVL{
    struct Node{
        Key value;
        int height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
        std::atomic<Node*> p;
        cds::sync::spin_lock<cds::backoff::LockDefault> mtx;
        bool routing;
        bool deleted;

        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        void update_children(){
            Node* L = l.load(mmo);
            Node* R = r.load(mmo);
            if(L){
                L->p.store(this);
            }
            if(R){
                R->p.store(this);
            }
        }
        void recalculate_height(){
            Node* L = l.load(mmo);
            Node* R = r.load(mmo);
            int lh = 0;
            int rh = 0;
            if(L != nullptr){
                lh = L->height;
            }
            if(R != nullptr){
                rh = R->height;
            }
            height = 1 + std::max(lh, rh);

        }
        int get_balance_factor(){
            Node* L = l.load(mmo);
            Node* R = r.load(mmo);
            int lh = 0;
            int rh = 0;
            if(L != nullptr){
                lh = L->height;
            }
            if(R != nullptr){
                rh = R->height;
            }
            return lh - rh;
        }
        Node(const Node& other) {
            value = other.value;
            Node* L =  other.l.load(mmo);
            Node* R = other.r.load(mmo);
            Node* P = other.p.load(mmo);
            l = L;
            r = R;
            p = P;
            height = other.height;
            routing = other.routing;
            deleted = other.deleted;
        }
        Node(const Key& value, Node* parent): p(parent),
                                value(value), height(1), routing(false), deleted(false){
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
    };
    struct NodeDisposer{
        void operator()(Node* p){
            delete p;
        }
    };
    typename Node::Node_allocator alloc;
    Compare comp;
    std::atomic<Node*> header;
    std::mutex null_header_mtx;
//    typedef cds::details::Allocator<Node,  Node::Node_allocator> cxx_allocator;
public:
//    LinearAllocator* linearAllocator;
//    ConcurrentAVL(LinearAllocator* linearAllocator):linearAllocator(linearAllocator){};
    void destroy(Node* curr){
        if(curr==nullptr){
            return;
        }
        destroy(curr->l.load());
        destroy(curr->r.load());
        delete curr;
    }
    ~ConcurrentAVL(){
        destroy(header.load());
    }
private:
    bool equiv(const Key& a, const Key& b) const{
        return !comp(a, b) && !comp(b, a);
    }


    std::tuple<Node*, Node*, Node*>
    search(cds::gc::HP::GuardArray<3>& guard_array,
           const std::atomic<Node*>& root,
           const Key& key) const{
        Node* gparent = nullptr; Node* parent = nullptr;
        int step=0;
        Node* curr = guard_array.protect(step, root);
        while(curr){
            if(equiv(curr->value,key)){
                break;
            }
            gparent = parent; parent = curr;
            ++step;
            if(comp(key,curr->value)){
                curr = guard_array.protect(step%3,curr->l);
            }else{
                curr = guard_array.protect(step%3,curr->r);
            }
        }
        return {gparent, parent, curr};
    }

public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
        if(header.load(std::memory_order_seq_cst) == nullptr){
            return false;
        }
        cds::gc::HP::GuardArray<3> ga;
        auto [gparent, parent, curr] = search(ga, header, key);
//        auto [gparent, parent, curr] = search(header, key);
        return curr && !curr->routing; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert_new_node(Node* parent, const Key& key){
        {
            std::lock_guard lock(parent->mtx);
            if (parent->deleted) {
                return false;
            }
            if (comp(key, parent->value)) {
                if (parent->l.load(std::memory_order_seq_cst) != nullptr) {
                    return false;
                }
                parent->l.store(Node::createNode(alloc, key, parent), std::memory_order_seq_cst);
            } else {
                if (parent->r.load(std::memory_order_seq_cst) != nullptr) {
                    return false;
                }
                parent->r.store(Node::createNode(alloc, key, parent), std::memory_order_seq_cst);
            }
        }
//        fix(parent);
        return true;
    }
    bool insert_existing_node(Node* curr, const Key& key){
        std::lock_guard lock(curr->mtx);
        if(curr->deleted){//пока брали блокировку, кто-то удалил узел
            return false;
        }
        curr->routing = false;
        return true;
    }
    void insert(const Key& key) {
//        std::cout << "insert: "<<key << std::endl;
        while(true) { //повторяем операцию до тех пор, пока не сможем ее корректно выполнить
            if (header.load(std::memory_order_seq_cst) == nullptr) {
                std::lock_guard lock(null_header_mtx);
                if(header.load(std::memory_order_seq_cst) != nullptr){ //пока брали блокировку, кто-то занял header
                    continue;
                }
                header.store(Node::createNode(alloc, key, nullptr), std::memory_order_seq_cst);
                break;
            }
            cds::gc::HP::GuardArray<3> ga;
            auto [gparent, parent, curr] = search(ga, header, key);

            bool result = (!curr) ? insert_new_node(parent, key)
                                  : insert_existing_node(curr, key);
            if(result){
                break;
            }
        }
    }
    bool remove_with_two_child(Node* curr){
        std::lock_guard lock(curr->mtx);
        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }

        //пока брали блокировку, у вершины стало не 2 ребенка, нельзя делать логику удаления как для 2
        if((curr->l.load(std::memory_order_seq_cst) != nullptr) + (curr->r.load(std::memory_order_seq_cst) != nullptr) != 2){
            return false;
        }
        curr->routing = true;
        return true;
    }
    bool critical_section_remove_with_one_child(Node* curr, Node* parent,
                                                const Key& key){
        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }
        if(parent && parent->deleted){
            return false;//родитель удален, если привяжем ребенка к нему, он потеряется. Нужно начать операцию заново
        }
        if((curr->l.load(std::memory_order_seq_cst) != nullptr) + (curr->r.load(std::memory_order_seq_cst) != nullptr) != 1){
            return false; //пока брали блокировку, вершина изменила вид
        }
        // надо удалить curr и отдать его ребенка parent
        cds::gc::HP::Guard childGuard;
        Node* child;
        if(curr->l.load(mmo) != nullptr){
            child = childGuard.protect(curr->l);;
        }else{
            child = childGuard.protect(curr->r);;
        }
        curr->deleted = true;
        if(!parent){ //TODO можно заменить
            header.store(child, std::memory_order_seq_cst); //потенциальная ошибка todo
            child->p.store(header);
            cds::gc::HP::retire<NodeDisposer>(curr);
        }else {
            if (comp(key, parent->value)) {
                parent->l.store(child, std::memory_order_seq_cst);
                child->p.store(parent);
                cds::gc::HP::retire<NodeDisposer>(curr);
            } else {
                parent->r.store(child, std::memory_order_seq_cst);
                child->p.store(parent);
                cds::gc::HP::retire<NodeDisposer>(curr);
            }
        }
        return true;
    }
    bool remove_with_one_child(Node* curr, Node* parent,
                               const Key& key){
        //пояснение зачем такой if в аналогичной функции remove_with_zero_child
        if(parent){
            std::lock_guard lock1(parent->mtx);
            std::lock_guard lock2(curr->mtx);
            return critical_section_remove_with_one_child(curr, parent, key);
        }else{
            std::lock_guard lock(curr->mtx);
            return critical_section_remove_with_one_child(curr, parent, key);
        }

    }
    bool critical_section_remove_with_zero_child(Node* curr, Node* parent,
                                                 Node* gparent, const Key& key){
        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }
        if(gparent&& gparent->deleted){
            return false;//удален предок, нужно начать заново
        }
        if(parent&&parent->deleted){
            return false;
        }
        if((curr->l.load(std::memory_order_seq_cst) != nullptr) + (curr->r.load(std::memory_order_seq_cst) != nullptr) != 0){
            return false;//пока брали блокировку, вершина изменила вид
        }
        // узел это лист, возможно надо удалить серого родителя
        if(!parent){
            curr->deleted = true;
            header.store(nullptr, std::memory_order_seq_cst);
            cds::gc::HP::retire<NodeDisposer>(curr);
            return true;
        }
        if(parent->routing) {
            cds::gc::HP::Guard childGuard;
            Node* child;
            curr->deleted = true;
            parent->deleted = true;
            if(comp(key,parent->value)){
                child = childGuard.protect(parent->r); //надо отдать gparent
//                parent->l.store(nullptr, std::memory_order_seq_cst);
            }else{
                child = childGuard.protect(parent->l);
//                parent->r.store(nullptr, std::memory_order_seq_cst);
            }

            if(!gparent){ // TODO можно заменить
                header.store(child, std::memory_order_seq_cst);
                child->p.store(header);
            }else {
                if (comp(key,gparent->value)) {
                    gparent->l.store(child, std::memory_order_seq_cst);
                    child->p.store(gparent);
                } else {
                    gparent->r.store(child, std::memory_order_seq_cst);
                    child->p.store(gparent);
                }
            }
            cds::gc::HP::retire<NodeDisposer>(curr);
            cds::gc::HP::retire<NodeDisposer>(parent);
        }else{
            curr->deleted = true;
//            cds::gc::HP::retire<Node>(curr);
            if (comp(key,parent->value)) {
                parent->l.store(nullptr, std::memory_order_seq_cst);
            } else {
                parent->r.store(nullptr, std::memory_order_seq_cst);
            }
            cds::gc::HP::retire<NodeDisposer>(curr);
        }
        return true;
    }
    bool remove_with_zero_child(Node* curr, Node* parent,
                                Node* gparent, const Key& key){
        //Надо брать разное количество мьютексов в зависимости о ситуации
        //if из-за того что надо взять их с помощью RAII и нет пустого lock_guard TODO
        if(gparent && parent) {
            std::lock_guard lock1(gparent->mtx);
            std::lock_guard lock2(parent->mtx);
            std::lock_guard lock3(curr->mtx);
            return critical_section_remove_with_zero_child(curr, parent, gparent, key);
        }else if(parent){
            std::lock_guard lock1(parent->mtx);
            std::lock_guard lock2(curr->mtx);
            return critical_section_remove_with_zero_child(curr, parent, gparent, key);
        }else{
            std::lock_guard lock(curr->mtx);
            return critical_section_remove_with_zero_child(curr, parent, gparent, key);
        }
    }
    bool erase(const Key& key) {
        cds::gc::HP::GuardArray<3> ga;
        while (true) {
            auto [gparent, parent, curr] = search(ga,header, key);
//            auto [gparent, parent, curr] = search(header, key);
            if (!curr || curr->routing) {
                return false; // узла нет в дереве, операция ни на что не повлияла
            }

            int n_children = (curr->l.load(std::memory_order_seq_cst) != nullptr)
                             + (curr->r.load(std::memory_order_seq_cst) != nullptr);
            bool result;
            if (n_children == 2) {
                result = remove_with_two_child(curr);
            } else if (n_children == 1) {
                result = remove_with_one_child(curr, parent, key);
            } else {
                result = remove_with_zero_child(curr, parent, gparent, key);
            }
            if(result){
                break;
            }
        }
        return true; // узел был, а в результате операции удален
    }
    void update_op(Node* curr, std::string op){
//        if(curr) {
//            curr->pred_last_op = std::move(curr->last_op);
//            curr->last_op = op + std::to_string(++curr->version);
//        }
    }
    std::tuple<int, int, int, int> take_node_params(Node* curr){
//        guard guard_l;
//        guard guard_r;
        Node* l = curr->l.load(mmo);
        Node* r = curr->r.load(mmo);
        int l_height = l == nullptr? 0 : l->height;
        int r_height = r == nullptr? 0 : r->height;
        int new_curr_height = 1 + std::max(l_height, r_height);
        int balance_factor = l_height - r_height;
        return {l_height, r_height, new_curr_height, balance_factor};
    }

    Node* fix_height(Node* curr, Node* parent){
        std::lock_guard lock(curr->mtx);
        auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);
        if(curr->deleted){
            return nullptr;
        }
        if(balance_factor< -1 || balance_factor > 1){ //пока брали блокировку, в приоритете стало произвести поворот
            return curr;                              //прервать изменение высоты и повторить балансировку для curr
        }

        if(new_curr_height == curr->height){//пока брали блокировку, кто-то поправил высоту
            return nullptr;                 //закончить fix(теперь fix это не забота этого потока)
        }

        curr->height = new_curr_height;
//        update_op(curr, "fix_height");
        return parent; // теперь надо чинить родителя
    }
    void internal_right_rotate(Node* curr, Node* parent, Node* curr_l_child){
        //теперь parent указывает на левого ребенка curr
        guard guard_curr_l_child_r;
        Node* curr_l_child_r = guard_curr_l_child_r.protect(curr_l_child->r);
//        if(curr_l_child_r){
//            curr_l_child_r->mtx.lock();
//            if(curr_l_child_r->p != curr_l_child ||
//               curr_l_child->r.load() != curr_l_child_r){
//                curr_l_child_r->mtx.unlock();
//                return false;
//            }
//        }
//        Node* new_curr = new Node(curr->value, parent);
//        Node* new_curr_l_child = new Node(curr_l_child->value, new_curr);

        //передаем curr правого ребенка от curr_l_child
        curr->l.store(curr_l_child_r);
        if(curr_l_child_r!=nullptr) {
            curr_l_child_r->p.store(curr);
        }
        //curr опускается ниже и делается правым ребенком curr_l_child
        curr->p.store(curr_l_child);
        curr_l_child->r.store(curr);

        curr->recalculate_height();
        curr_l_child->recalculate_height();

        if(parent!=nullptr){
            curr_l_child->p.store(parent);
            if (comp(curr->value, parent->value)) {
                parent->l.store(curr_l_child);
            } else {
                parent->r.store(curr_l_child);
            }
        }else{
            curr_l_child->p.store(nullptr);
            header.store(curr_l_child);
        }
//        if(curr_l_child_r) {
//            curr_l_child_r->mtx.unlock();
//        }
//        return true;
    }

    void internal_left_rotate(Node* curr, Node* parent, Node* curr_r_child){
        //теперь parent указывает на правого ребенка curr
        guard guard_curr_r_child_l;
        Node* curr_r_child_l = guard_curr_r_child_l.protect(curr_r_child->l);
//        if(curr_r_child_l){
//            curr_r_child_l->mtx.lock();
//            if(curr_r_child_l->p != curr_r_child ||
//            curr_r_child->l.load() != curr_r_child_l){
//                curr_r_child_l->mtx.unlock();
//                return false;
//            }
//        }

        //передаем curr левого ребенка от curr_r_child
        curr->r.store(curr_r_child_l);
        if(curr_r_child_l!=nullptr) {
            curr_r_child_l->p.store(curr);
        }
        //curr опускается ниже и делается правым ребенком curr_l_child
        curr->p.store(curr_r_child);
        curr_r_child->l.store(curr);

        curr->recalculate_height();
        curr_r_child->recalculate_height();

        if(parent!=nullptr) {
            curr_r_child->p.store(parent);
            if (comp(curr->value, parent->value)) {
                parent->l.store(curr_r_child);
            } else {
                parent->r.store(curr_r_child);
            }
        }else{
            curr_r_child->p.store(nullptr);
            header.store(curr_r_child);
        }
//        if(curr_r_child_l){
//            curr_r_child_l->mtx.unlock();
//        }
//        return true;
    }
//    Node* small_right_rotate(Node* curr, Node* parent, Node* curr_l_child, Node* c){
//        if(internal_right_rotate(curr, parent, curr_l_child)){
//            return parent;
//        }else{
//            return curr;
//        }
////        update_op(curr, "small_right_rotate_curr");
////        update_op(parent, "small_right_rotate_parent");
////        update_op(curr_l_child, "small_right_rotate_curr_l_child");
////        return parent;
//    }

    Node* small_right_rotate(Node* curr, Node* parent, Node* curr_l_child){

        Node* new_curr = new Node(*curr);
        Node* new_curr_l_child = new Node(*curr_l_child);
        new_curr->l.store(new_curr_l_child);
        new_curr_l_child->p.store(new_curr);

        internal_right_rotate(new_curr, parent, new_curr_l_child);

        curr->deleted = true;
        curr_l_child->deleted = true;
        new_curr_l_child->update_children();
        new_curr->update_children();
        cds::gc::HP::retire<NodeDisposer>(curr);
        cds::gc::HP::retire<NodeDisposer>(curr_l_child);
        return parent;
    }
    Node* big_right_rotate(Node* curr, Node* parent, Node* curr_l_child, Node* curr_l_child_r){
        //тут уже должна быть взята блокировка на curr и parent и curr_l_child
//        int l_balance_factor = curr_l_child == nullptr ? 0 : curr_l_child->get_balance_factor();
//        if(l_balance_factor>=0){//пока брали блокировку кто-то сбалансировал детей curr_l_child
//            return small_right_rotate(curr, parent, curr_l_child);
//        }

//        Node* curr_l_child_r = curr_l_child->r.load();
//        assert(curr_l_child->r.load() == curr_l_child_r);

        if(curr_l_child_r && curr_l_child_r->p.load(mmo) != curr_l_child){
            return curr;
        }
        if(curr_l_child->r.load(mmo) != curr_l_child_r){//maybe not need TODO
            return curr;
        }
//        assertcurr_l_child_r->p.load() == curr_l_child);


        Node* new_curr = new Node(*curr);
        Node* new_curr_l_child = new Node(*curr_l_child);
        Node* new_curr_l_child_r = new Node(*curr_l_child_r);
        new_curr->l.store(new_curr_l_child);
        new_curr_l_child->p.store(new_curr);
        new_curr_l_child->r.store(new_curr_l_child_r);
        new_curr_l_child_r->p.store(new_curr_l_child);

        internal_left_rotate(new_curr_l_child, new_curr, new_curr_l_child_r);

        internal_right_rotate(new_curr, parent, new_curr_l_child_r);

        curr->deleted = true;
        curr_l_child->deleted = true;
        curr_l_child_r->deleted = true;
        new_curr_l_child_r->update_children();
        new_curr_l_child->update_children();
        new_curr->update_children();
        cds::gc::HP::retire<NodeDisposer>(curr);
        cds::gc::HP::retire<NodeDisposer>(curr_l_child);
        cds::gc::HP::retire<NodeDisposer>(curr_l_child_r);
        return parent;
    }
    Node* right_rotate(cds::gc::HP::GuardArray<4>& ga, Node* curr, Node* parent, Node* curr_l_child){
        //тут уже должна быть взята блокировка на curr и parent и curr_l_child
        if(curr_l_child){
            if(curr_l_child->p.load() != curr){
                return curr;
            }
        }
        if(curr->l.load(mmo) != curr_l_child){ //maybe not need TODO
            return curr;
        }
        int l_balance_factor = curr_l_child == nullptr ? 0 : curr_l_child->get_balance_factor();
        if(l_balance_factor >= 0){
            return small_right_rotate(curr, parent, curr_l_child); //curr и parent уже взяты в lock
        }else{
//            guard guard_curr_l_child_r;
            Node* curr_l_child_r = ga.protect(3,curr_l_child->r);
            if(curr_l_child_r) {
                std::lock_guard lock(curr_l_child_r->mtx);
                return big_right_rotate(curr, parent, curr_l_child, curr_l_child_r);
            }else{
                return big_right_rotate(curr, parent, curr_l_child, curr_l_child_r);
            }
        }
    }
//    Node* small_left_rotate(Node* curr, Node* parent, Node* curr_r_child, Node* c){
//        if(!internal_left_rotate(curr, parent, curr_r_child)){
//            return curr;
//        }
////        curr->pred_last_op = curr->last_op;
////        update_op(curr, "small_left_rotate_curr");
////        update_op(parent, "small_left_rotate_parent");
////        update_op(curr_r_child, "small_left_rotate_curr_r_child");
//        return parent;
//    }
    Node* small_left_rotate(Node* curr, Node* parent, Node* curr_r_child){
        Node* new_curr = new Node(*curr);
        Node* new_curr_r_child = new Node(*curr_r_child);
        new_curr->r.store(new_curr_r_child);
        new_curr_r_child->p.store(new_curr);
        internal_left_rotate(new_curr, parent, new_curr_r_child);

        curr->deleted = true;
        curr_r_child->deleted = true;
        new_curr_r_child->update_children();
        new_curr->update_children();
        cds::gc::HP::retire<NodeDisposer>(curr);
        cds::gc::HP::retire<NodeDisposer>(curr_r_child);
        return parent;
    }
    Node* big_left_rotate(Node* curr, Node* parent, Node* curr_r_child,Node* curr_r_child_l){
        //тут уже должна быть взята блокировка на curr и parent
//        std::lock_guard lock(curr_r_child->mtx);
//
//        int r_balance_factor = curr_r_child == nullptr ? 0 : curr_r_child->get_balance_factor();
//        if(r_balance_factor<=0){//пока брали блокировку кто-то сбалансировал детей curr_l_child
//            return small_left_rotate(curr, parent, curr_r_child);
//        }
//
//        Node* curr_r_child_l = curr_r_child->l.load();

        if(curr_r_child_l && curr_r_child_l->p.load() != curr_r_child){
            return curr;
        }
        if(curr_r_child->l.load() != curr_r_child_l){//maybe not need TODO
            return curr;
        }
//        assert(curr_r_child_l->p.load() == curr_r_child);

        Node* new_curr = new Node(*curr);
        Node* new_curr_r_child = new Node(*curr_r_child);
        Node* new_curr_r_child_l = new Node(*curr_r_child_l);
        new_curr->r.store(new_curr_r_child);
        new_curr_r_child->p.store(new_curr);
        new_curr_r_child->l.store(new_curr_r_child_l);
        new_curr_r_child_l->p.store(new_curr_r_child);


        internal_right_rotate(new_curr_r_child, new_curr, new_curr_r_child_l);
        internal_left_rotate(new_curr, parent, new_curr_r_child_l);

        curr->deleted = true;
        curr_r_child->deleted = true;
        curr_r_child_l->deleted = true;
        new_curr_r_child_l->update_children();
        new_curr_r_child->update_children();
        new_curr->update_children();
        cds::gc::HP::retire<NodeDisposer>(curr);
        cds::gc::HP::retire<NodeDisposer>(curr_r_child);
        cds::gc::HP::retire<NodeDisposer>(curr_r_child_l);
        return parent;
    }
    Node* left_rotate(cds::gc::HP::GuardArray<4>& ga, Node* curr, Node* parent,  Node* curr_r_child){
        //тут уже должна быть взята блокировка на curr и parent
//        Node* curr_r_child = curr->r.load();
        if(curr_r_child){
            if(curr_r_child->p.load() != curr){
                return curr;
            }
        }
        if(curr->r.load() != curr_r_child){ //maybe not need TODO
            return curr;
        }

        int r_balance_factor = curr_r_child == nullptr ? 0 : curr_r_child->get_balance_factor();
        if(r_balance_factor <= 0){
            return small_left_rotate(curr, parent, curr_r_child); //curr и parent уже взяты в lock
        }else{
//            guard guard_curr_r_child_l;
            Node* curr_r_child_l = ga.protect(3, curr_r_child->l);
            if(curr_r_child_l) {
                std::lock_guard lock(curr_r_child_l->mtx);
                return big_left_rotate(curr, parent,curr_r_child, curr_r_child_l);
            }else{
                return big_left_rotate(curr, parent,curr_r_child, curr_r_child_l);
            }
        }
    }
    Node* fix_balance(cds::gc::HP::GuardArray<4>& ga, Node* curr, Node* parent){
        //нужно чтобы уже был взят блок на curr и parent(если он не nullptr)
        if(curr->deleted){//пока брали блокировку curr удален
            return nullptr;//заканчиваем чинить дерево
        }
        if(curr->p.load(mmo) != parent){
            return curr;
        }
        if(parent ){
            //пока брали блокировку с родителем стало плохо
            if(parent->deleted){
                return curr;  //повторить операцию
            }
            if(comp(curr->value, parent->value)){
                if(parent->l.load(mmo)!=curr){
                    return curr;
                }
            }else{
                if(parent->r.load(mmo)!=curr){
                    return curr;
                }
            }
        }
        auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);
        if(balance_factor > 1){
//            guard guard_curr_l_child;
            Node* curr_l_child = ga.protect(2, curr->l);
            if(curr_l_child) {
                std::lock_guard lock(curr_l_child->mtx);
                return right_rotate(ga,curr, parent, curr_l_child);
            }else{
                return right_rotate(ga,curr, parent, curr_l_child);
            }
        }else if(balance_factor < -1){
//            guard guard_curr_r_child;
            Node* curr_r_child = ga.protect(2, curr->r);
            if(curr_r_child){
                std::lock_guard lock(curr_r_child->mtx);
                return left_rotate(ga, curr, parent, curr_r_child);
            }else {
                return left_rotate(ga, curr, parent, curr_r_child);
            }
        }else{ // пока брали блокировку, кто-то поправил баланс, возможно надо поправить высоту(раз уже взяли блокировку)
            if(curr->height == new_curr_height){ // высоту тоже кто-то поправил
                return nullptr;                  // закончить fix(теперь fix это не забота этого потока)
            }

            curr->height = new_curr_height;
            return parent; // теперь надо чинить родителя
        }
    }
    void fix(Node* curr){
        cds::gc::HP::GuardArray<4> ga;
        int c =0;
        while(curr != nullptr && !curr->deleted){

            auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);
//            curr_grd.assign(curr);
            Node* parent = ga.protect(c%2, curr->p);
            if(balance_factor>=-1 && balance_factor <=1){//с высотами детей все в порядке
                if(new_curr_height == curr->height){
                    //кто-то все починил
                    return;
                }
                if(parent != nullptr) {
                    curr = fix_height(curr, parent);// корректируем свою высоту и получаем следующий узел для балансировки
                }else{
                    std::lock_guard lock1(null_header_mtx);
                    curr = fix_height(curr, parent);
                }
            }else{
                if(parent!=nullptr) {
                    std::lock_guard lock1(parent->mtx);
                    if(parent->l.load() != curr && parent->r.load()!= curr) continue;
                    std::lock_guard lock2(curr->mtx);
                    curr = fix_balance(ga, curr, parent);
                }else{
                    std::lock_guard lock1(null_header_mtx);
                    std::lock_guard lock2(curr->mtx);
                    curr = fix_balance(ga, curr, parent);
                }
            }
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
        }
    }
    void check_heights(){
        check_heights(header.load());
    }

    void print(Node* curr, int dep, char ch){
        if(curr == nullptr){
            return;
        }
        print(curr->l.load(), dep + 1, '/');
        std::cout.width(dep * 5);
        std::cout << ch << "(" << curr->value << ',' <<curr->get_balance_factor() << ") \n";
        print(curr->r.load(), dep + 1, '\\');
    }
    void print(){
        print(header.load(), 0, '=');
    }
};

#endif //BACHELOR_CONCURRENTAVL_H
