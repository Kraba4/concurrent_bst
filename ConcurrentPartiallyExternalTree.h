//
// Created by HONOR on 24.04.2023.
//
#include <functional>
#include <memory>
#include <mutex>
#include <cds/sync/spinlock.h>
#include <cds/gc/hp.h>

#ifndef BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
#define BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H

template<typename T>
using guarded_ptr = cds::gc::HP::guarded_ptr<T>;

using guard = cds::gc::HP::Guard;
template <typename Key, typename Compare = std::less<Key>, typename Alloc = std::allocator<Key>>
class ConcurrentPartiallyExternalTree{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key value;
        int height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
        cds::sync::spin_lock<cds::backoff::LockDefault> mtx;

        /* информация для partially external, если true, то вершина считается вспомогательной для навигации
         * логически в контейнере ее нет*/
        bool routing;

        /* метка того, что вершина удалена(недостижима в дереве). Нужно потому, что пока один поток
         * нашел вершину и собирается начать работать с ней, другой поток может удалить ее из дерева*/
        bool deleted;


        Node(const Key& value): l(), r(),
                                        value(value), height(0), routing(false), deleted(false){
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
    Compare comp;
    std::atomic<Node*> header;
    std::mutex null_header_mtx;
public:
    int retries=0;
    ConcurrentPartiallyExternalTree(Alloc alloc = Alloc()): alloc(alloc){}
    ~ConcurrentPartiallyExternalTree(){
        header.store(nullptr);
    }
private:
    bool equiv(const Key& a, const Key& b) const{
        return !comp(a, b) && !comp(b, a);
    }

    //Функция поиска узла, его родителя и родителя родителя по ключу.
    std::tuple<guard, guard, guard, Node*, Node*, Node*>
    search(const std::atomic<Node*>& root, const Key& key) {
        guard gparent_grd;
        guard parent_grd;
        guard curr_grd;
        Node* gparent = nullptr;
        Node* parent = nullptr;
        Node* curr = curr_grd.protect(root);

        while(curr){
            if(equiv(curr->value,key)){
                break;
            }

            // спускаемся по дереву
            gparent = std::move(parent);
            parent = std::move(curr);
            gparent_grd.copy(parent_grd);
            parent_grd.copy(curr_grd);
            if(comp(key,parent->value)){
                curr = curr_grd.protect(parent->l);
            }else{
                curr = curr_grd.protect(parent->r);
            }
        }
        return {std::move(gparent_grd), std::move(parent_grd), std::move(curr_grd), std::move(gparent), std::move(parent), std::move(curr)};
    }

public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
        if(header.load(std::memory_order_relaxed) == nullptr){
            return false;
        }
        auto [gp_grd, p_grd, c_grd, gparent, parent, curr] = search(header, key);
        return curr && !curr->routing; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert_new_node(Node* parent, const Key& key){
        std::lock_guard lock(parent->mtx);
        if(parent->deleted) {//пока брали блокировку, кто-то удалил узел
            return false;
        }
        if (comp(key,parent->value)) { // смотрим куда надо поставить Node(key)
            if(parent->l.load(std::memory_order_relaxed) != nullptr) { //пока брали блокировку, кто-то занял нужное место
                return false;
            }
            //так как взята блокировка, никто не может изменить parent->l с момента проверки условия != nullptr
            parent->l.store(Node::createNode(alloc, key), std::memory_order_relaxed);
        } else {
            if(parent->r.load(std::memory_order_relaxed) != nullptr) {
                return false;
            }
            parent->r.store(Node::createNode(alloc, key), std::memory_order_relaxed);
        }
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
        while(true) { //повторяем операцию до тех пор, пока не сможем ее корректно выполнить
            if (header.load(std::memory_order_relaxed) == nullptr) {
                std::lock_guard lock(null_header_mtx);
                if(header.load(std::memory_order_relaxed) != nullptr){ //пока брали блокировку, кто-то занял header
                    continue;
                }
                header.store(Node::createNode(alloc, key), std::memory_order_relaxed);
                break;
            }
            auto [gp_grd, p_grd, c_grd, gparent, parent, curr] = search(header, key);

            bool result;
            if (!curr) {//узла с нужным ключем нет в дереве
                result = insert_new_node(parent, key);
            } else {//узел есть, но может быть routing
                result = insert_existing_node(curr, key);
            }
            if(result){ //никакой поток не помешал операции, можно выходить из цикла
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
        if((curr->l.load(std::memory_order_relaxed) != nullptr) + (curr->r.load(std::memory_order_relaxed) != nullptr) != 2){
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
        if((curr->l.load(std::memory_order_relaxed) != nullptr) + (curr->r.load(std::memory_order_relaxed) != nullptr) != 1){
            return false; //пока брали блокировку, вершина изменила вид
        }
        // надо удалить curr и отдать его ребенка parent
        cds::gc::HP::Guard childGuard;
        Node* child;
        if(curr->l.load() != nullptr){
            childGuard.protect(curr->l);
            child = curr->l.load(std::memory_order_relaxed);
        }else{
            childGuard.protect(curr->r);
            child = curr->r.load(std::memory_order_relaxed);
        }
        curr->deleted = true;
        if(!parent){ //TODO можно заменить
            header.store(child, std::memory_order_release); //потенциальная ошибка todo
            cds::gc::HP::retire<NodeDisposer>(curr);
        }else {
            if (comp(key, parent->value)) {
                parent->l.store(child, std::memory_order_release);
                cds::gc::HP::retire<NodeDisposer>(curr);
            } else {
                parent->r.store(child, std::memory_order_release);
                cds::gc::HP::retire<NodeDisposer>(curr);
            }
        }
        return true;
    }
    bool remove_with_one_child(Node* curr, Node* parent,
                               const Key& key){
        //пояснение зачем такой if в аналогичной функции remove_with_zero_child
        if(parent){
            std::scoped_lock lock(parent->mtx, curr->mtx);
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
        if((curr->l.load(std::memory_order_relaxed) != nullptr) + (curr->r.load(std::memory_order_relaxed) != nullptr) != 0){
            return false;//пока брали блокировку, вершина изменила вид
        }
        // узел это лист, возможно надо удалить серого родителя
        if(!parent){
            curr->deleted = true;
            header.store(nullptr, std::memory_order_release);
            cds::gc::HP::retire<NodeDisposer>(curr);
            return true;
        }
        if(parent->routing) {
            cds::gc::HP::Guard childGuard;
            Node* child;
            curr->deleted = true;
            parent->deleted = true;
            if(comp(key,parent->value)){
                childGuard.protect(parent->r);
                child = parent->r.load(std::memory_order_relaxed); //надо отдать gparent
                parent->l.store(nullptr, std::memory_order_release);
            }else{
                childGuard.protect(parent->l);
                child = parent->l.load(std::memory_order_relaxed);
                parent->r.store(nullptr, std::memory_order_release);
            }

            if(!gparent){ // TODO можно заменить
                header.store(child, std::memory_order_release);
            }else {
                if (comp(key,gparent->value)) {
                    gparent->l.store(child, std::memory_order_release);
                } else {
                    gparent->r.store(child, std::memory_order_release);
                }
            }
            cds::gc::HP::retire<NodeDisposer>(curr);
            cds::gc::HP::retire<NodeDisposer>(parent);
        }else{
            curr->deleted = true;
//            cds::gc::HP::retire<Node>(curr);
            if (comp(key,parent->value)) {
                parent->l.store(nullptr, std::memory_order_release);
            } else {
                parent->r.store(nullptr, std::memory_order_release);
            }
            cds::gc::HP::retire<NodeDisposer>(curr);
        }
        return true;
    }
    bool remove_with_zero_child(Node* curr, Node* parent,
                              Node* gparent, const Key& key){
        //Надо брать разное количество мьютексов в зависимости о ситуации
        //if из-за того что надо взять их с помощью RAII и нет пустого scoped_lock
        if(gparent && parent) {
            std::scoped_lock lock(gparent->mtx,parent->mtx,curr->mtx);
            return critical_section_remove_with_zero_child(curr, parent, gparent, key);
        }else if(parent){
            std::scoped_lock lock(parent->mtx,curr->mtx);
            return critical_section_remove_with_zero_child(curr, parent, gparent, key);
        }else{
            std::lock_guard lock(curr->mtx);
            return critical_section_remove_with_zero_child(curr, parent, gparent, key);
        }
    }
    bool erase(const Key& key) {
        while (true) {
            auto [gp_grd, p_grd, c_grd, gparent, parent, curr] = search(header, key);
            if (!curr || curr->routing) {
                return false; // узла нет в дереве, операция ни на что не повлияла
            }

            int n_children = (curr->l.load(std::memory_order_relaxed) != nullptr)
                             + (curr->r.load(std::memory_order_relaxed) != nullptr);
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
    void print(Node* next){
        if(next==nullptr){
            return;
        }
        print(next->l.load());
        print(next->r.load());
        retries++;
    }
    void print(){
        std::cout << std::endl;
        if(header.load()==nullptr){
            std::cout << "onnno";
        }
        print(header.load());
        std::cout << std::endl;
    }
};

#endif //BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
