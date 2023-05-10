//
// Created by HONOR on 24.04.2023.
//
#include <functional>
#include <memory>
#include <mutex>
#include "atomic_shared_ptr.h"
#include <cds/gc/hp.h>

#ifndef BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
#define BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H

template<typename T>
using SharedPtr = LFStructs::SharedPtr<T>;

template<typename T>
using AtomicSharedPtr = LFStructs::AtomicSharedPtr<T>;

template<typename T>
using guarded_ptr = cds::gc::HP::guarded_ptr<T>;

template <typename Key, typename Compare = std::less<Key>,
          typename Alloc = std::allocator<Key>>
class ConcurrentPartiallyExternalTree{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key value;
        int height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
//        std::atomic<LFStructs::SharedPtr<Node>> l;
//        std::atomic<LFStructs::SharedPtr<Node>> r;
        std::mutex mtx;

        /* информация для partially external, если true, то вершина считается вспомогательной для навигации
         * логически в контейнере ее нет*/
        bool routing;

        /* метка того, что вершина удалена(недостижима в дереве). Нужно потому, что пока один поток
         * нашел вершину и собирается начать работать с ней, другой поток может удалить ее из дерева*/
        bool deleted;


        Node(const Key& value): l(), r(),
                                        value(value), height(0), routing(false), deleted(false){
        }
        ~Node(){
//            std::cout << "destroy " << value << std::endl;
            l = SharedPtr<Node>();
            r = SharedPtr<Node>();
        }
        template<typename ...Args>
        static Node* createNode(Node_allocator& alloc, Args&&... args){
//            return LFStructs::SharedPtr<Node> std::allocate_shared<Node>(alloc,std::forward<Args>(args)...).get());
            Node* p = Alloc_traits::allocate(alloc, 1);
            Alloc_traits::construct(alloc, p, std::forward<Args>(args)...);
            return p;
        }
//        static void deleteNode(Node_allocator& alloc, LFStructs::SharedPtr<Node> node){
//            Alloc_traits::destroy(alloc, node);
//            Alloc_traits::deallocate(alloc, node, 1);
//        }
    };
    typename Node::Node_allocator alloc;
    Compare comp;
    std::atomic<Node*> header;
    std::mutex null_header_mtx;
public:
    int retries=0;
    ConcurrentPartiallyExternalTree(Alloc alloc = Alloc()): alloc(alloc){
    }
    ~ConcurrentPartiallyExternalTree(){
//        std::cout << "tree" << std::endl;
        header.store(nullptr);
    }
private:
    bool equiv(const Key& a, const Key& b) const{
        return !comp(a, b) && !comp(b, a);
    }
    //Функция поиска узла, его родителя и родителя родителя по ключу.
    std::tuple<guarded_ptr<Node>, guarded_ptr<Node>, guarded_ptr<Node>>
    search(const std::atomic<Node*>& root, const Key& key) {
        guarded_ptr<Node> gparent;
        guarded_ptr<Node> parent;
        guarded_ptr<Node> curr(root);

//        std::cout << "[ ";
        while(!curr.empty()){
//            std::cout << curr->value << ' ';
            if(equiv(curr->value,key)){
                break;
            }

            // спускаемся по дереву
            gparent = std::move(parent);
            parent = std::move(curr);
            if(comp(key,parent->value)){
                curr = guarded_ptr<Node>(parent->l);
            }else{
                curr = guarded_ptr<Node>(parent->r);
            }
        }
//        std::cout << "] ";
        return {std::move(gparent), std::move(parent), std::move(curr)};
    }

public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
//        std::cout << "\ncontains " << key << ' ';
        if(header.load() == nullptr){
            return false;
        }
        auto [gparent, parent, curr] = search(header, key);
//        std::cout <<  bool(curr!=nullptr && !curr->routing);
        return curr && !curr->routing; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert_new_node(const guarded_ptr<Node>& parent, const Key& key){
        std::lock_guard<std::mutex> lock(parent->mtx);
        if(parent->deleted) {//пока брали блокировку, кто-то удалил узел
            return false;
        }
        if (comp(key,parent->value)) { // смотрим куда надо поставить Node(key)
            if(parent->l.load() != nullptr) { //пока брали блокировку, кто-то занял нужное место
                return false;
            }
            //так как взята блокировка, никто не может изменить parent->l с момента проверки условия != nullptr
            parent->l.store(Node::createNode(alloc, key));
        } else {
            if(parent->r.load() != nullptr) {
                return false;
            }
            parent->r.store(Node::createNode(alloc, key));
        }
        return true;
    }
    bool insert_existing_node(const guarded_ptr<Node>& curr, const Key& key){
        std::lock_guard<std::mutex> lock(curr->mtx);
        if(curr->deleted){//пока брали блокировку, кто-то удалил узел
            return false;
        }
        curr->routing = false;
        return true;
    }
    void insert(const Key& key) {
        while(true) { //повторяем операцию до тех пор, пока не сможем ее корректно выполнить
//            std::cout << "\ninsert " << key << ' ';
            if (header.load() == nullptr) {
                std::lock_guard lock(null_header_mtx);
                if(header.load() != nullptr){ //пока брали блокировку, кто-то занял header
                    continue;
                }
                header.store(Node::createNode(alloc, key));
                break;
            }
            auto [gparent, parent, curr] = search(header, key);

            bool result;
            if (!curr) {//узла с нужным ключем нет в дереве
                result = insert_new_node(parent, key);
            } else {//узел есть, но может быть routing
                result = insert_existing_node(curr, key);
            }
            if(result){ //никакой поток не помешал операции, можно выходить из цикла
                break;
            }else{
                retries++;
            }
        }
    }
    bool remove_with_two_child(const guarded_ptr<Node>& curr){
        std::lock_guard lock(curr->mtx);
        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }

        //пока брали блокировку, у вершины стало не 2 ребенка, нельзя делать логику удаления как для 2
        if((curr->l.load() != nullptr) + (curr->r.load() != nullptr) != 2){
            return false;
        }
        curr->routing = true;
        return true;
    }
    bool remove_with_one_child(const guarded_ptr<Node>& curr, const guarded_ptr<Node>& parent,
                               const Key& key){
        std::mutex dummy_parent_mtx;
        auto lock = std::scoped_lock(dummy_parent_mtx, curr->mtx);
//        auto lock = (parent) ? std::scoped_lock(parent->mtx, curr->mtx):
//                                                std::scoped_lock(dummy_parent_mtx, curr->mtx);
        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }
        if(parent && parent->deleted){
            return false;//родитель удален, если привяжем ребенка к нему, он потеряется. Нужно начать операцию заново
        }
        if((curr->l.load() != nullptr) + (curr->r.load() != nullptr) != 1){
            return false; //пока брали блокировку, вершина изменила вид
        }
        // надо удалить curr и отдать его ребенка parent
        cds::gc::HP::Guard childGuard;
        Node* child;
        if(curr->l.load() != nullptr){
            childGuard.protect(curr->l);
            child = curr->l.load();
        }else{
            childGuard.protect(curr->r);
            child = curr->r.load();
        }
        curr->deleted = true;
//        cds::gc::HP::retire<Node>(curr.load());
        if(!parent){ //TODO можно заменить
            header.store(child); //потенциальная ошибка todo
        }else {
            if (comp(key, parent->value)) {
                parent->l.store(child);
            } else {
                parent->r.store(child);
            }
        }
        return true;
    }
    bool remove_with_zero_child(const guarded_ptr<Node>& curr, const guarded_ptr<Node>& parent,
                              const guarded_ptr<Node>& gparent, const Key& key){
        //на случай если gparent == nullptr parent==nullptr,
        //ведь надо взять блокировку только у существующих узлов
        //dummy_mtx для того, чтобы взять блокировку на несуществующий mutex(пустого mutex не существует)
        std::mutex dummy_gparent_mtx;
        std::mutex dummy_parent_mtx;
        auto lock = std::scoped_lock(dummy_gparent_mtx, dummy_parent_mtx, curr->mtx);
//        auto lock =
//        (gparent && parent)? std::scoped_lock(gparent->mtx,parent->mtx,curr->mtx):
//        (parent)?            std::scoped_lock(dummy_gparent_mtx, parent->mtx, curr->mtx):
//                             std::scoped_lock(dummy_gparent_mtx, dummy_parent_mtx, curr->mtx);

        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }
        if(gparent&& gparent->deleted){
            return false;//удален предок, нужно начать заново
        }
        if(parent&&parent->deleted){
            return false;
        }
        if((curr->l.load() != nullptr) + (curr->r.load() != nullptr) != 0){
            return false;//пока брали блокировку, вершина изменила вид
        }
        // узел это лист, возможно надо удалить серого родителя
        if(!parent){
            curr->deleted = true;
            header.store(nullptr);
            return true;
        }
        if(parent->routing) {
            cds::gc::HP::Guard childGuard;
            Node* child;
            if(comp(key,parent->value)){
                childGuard.protect(parent->r);
                child = parent->r.load(); //надо отдать gparent
            }else{
                childGuard.protect(parent->l);
                child = parent->l.load();
            }
            curr->deleted = true;
            parent->deleted = true;
//            cds::gc::HP::retire<Node>(curr);
//            cds::gc::HP::retire<Node>(parent);
            if(!gparent){ // TODO можно заменить
                header.store(child);
            }else {
                if (comp(key,gparent->value)) {
                    gparent->l.store(child);
                } else {
                    gparent->r.store(child);
                }
            }
        }else{
            curr->deleted = true;
//            cds::gc::HP::retire<Node>(curr);
            if (comp(key,parent->value)) {
                parent->l.store(nullptr);
            } else {
                parent->r.store(nullptr);
            }
        }
        return true;
    }
    bool remove(const Key& key) {
//        auto [gparent, parent, curr] = search(header, key);//находим один раз
        //                                                   если кто-то удалит, значит выполнил за нас
        while (true) {
            auto [gparent, parent, curr] = search(header, key);
//            std::cout << "\nremove " << key << ' ';
            if (!curr || curr->routing) {
                return false; // узла нет в дереве, операция ни на что не повлияла
            }

            int n_children = (curr->l.load() != nullptr)
                             + (curr->r.load() != nullptr);
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
            }else{
                retries++;
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
//        std::cout << next->value << ' ';
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
