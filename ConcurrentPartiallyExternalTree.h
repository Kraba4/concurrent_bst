//
// Created by HONOR on 24.04.2023.
//

#ifndef BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
#define BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H

#include <functional>
#include <memory>
template <typename Key, typename Compare = std::less<Key>,
          typename Alloc = std::allocator<Key>>
class ConcurrentPartiallyExternalTree{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key value;
        std::atomic<std::shared_ptr<Node>> l;
        std::atomic<std::shared_ptr<Node>> r;
        std::atomic<std::shared_ptr<Node>> p;
        int height;
        bool deleted;
        Node(std::shared_ptr<Node> parent, const Key& value): p(parent), l(nullptr), r(nullptr),
                                        value(value), height(0), deleted(false){}
        template<typename ...Args>
        static std::shared_ptr<Node> createNode(Node_allocator& alloc, Args&&... args){
            Node* node = Alloc_traits::allocate(alloc, 1);
            Alloc_traits::construct(alloc, node, std::forward<Args>(args)...);
            return std::shared_ptr<Node>(node);
        }
//        static void deleteNode(Node_allocator& alloc, std::shared_ptr<Node> node){
//            Alloc_traits::destroy(alloc, node);
//            Alloc_traits::deallocate(alloc, node, 1);
//        }
    };
    typename Node::Node_allocator alloc;
    std::atomic<std::shared_ptr<Node>> header;
    Compare comp;
public:
    ConcurrentPartiallyExternalTree(Alloc alloc = Alloc()): alloc(alloc){}

private:
    bool equiv(const Key& a, const Key& b) const{
        return !comp(a, b) && !comp(b, a);
    }
    //Функция поиска узла, его родителя и родителя родителя по ключу.
    std::tuple<std::shared_ptr<Node>, std::shared_ptr<Node>, std::shared_ptr<Node>>
    search(std::shared_ptr<Node> root, const Key& key) const {
        std::shared_ptr<Node> gparent = nullptr;
        std::shared_ptr<Node> parent = nullptr;
        std::shared_ptr<Node> curr = root;
        std::cout << "[ ";
        while(curr != nullptr){
            std::cout << curr->value << ' ';
            if(equiv(curr->value,key)){
                break;
            }

            // спускаемся по дереву
            gparent = parent;
            parent = curr;
            if(comp(key,curr->value)){
                curr = curr->l.load();
            }else{
                curr = curr->r.load();
            }
        }
        std::cout << "] ";
        return {std::move(gparent), std::move(parent), std::move(curr)};
    }

public:
    bool contains(const Key& key) const{
        std::cout << "\ncontains " << key << ' ';
        auto [gparent, parent, curr] = search(header, key);
        std::cout <<  bool(curr!=nullptr && !curr->deleted);
        return curr!=nullptr && !curr->deleted; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert(const Key& key) {
        if(header.load() == nullptr){
            header.store(Node::createNode(alloc, nullptr, key));
            return true;
        }
        std::cout << "\ninsert " << key << ' ';
        auto [gparent, parent, curr] = search(header, key);
        if(curr == nullptr){ //узла с нужным ключем нет в дереве
            if(comp(key,parent->value)){ // возвращаемся к родителю и ищем чем был curr(левым или правым ребенком)
                parent->l.store(Node::createNode(alloc, parent, key));
            }else{
                parent->r.store(Node::createNode(alloc, parent, key));
            }
            return true; // узла не было, а в результате операции появился
        }else{
            if(curr->deleted){
                curr->deleted = false;
                return true; // узла не было, а в результате операции появился
            }else {
                return false; // узел и так был, операция ни на что не повлияла
            }
        }
    }
    bool remove(const Key& key) {
        std::cout << "\nremove " << key << ' ';
        auto [gparent, parent, curr] = search(header, key);
        if(curr == nullptr || curr->deleted){
            return false; // узла нет в дереве, операция ни на что не повлияла
        }
        int n_children = (curr->l.load() != nullptr) + (curr->r.load() != nullptr);
        if(n_children == 2){
            // у вершины оба ребенка есть, надо только пометить его удаленным
            curr->deleted = true;
        }else if(n_children == 1){
            // надо удалить curr и отдать его ребенка parent
            std::shared_ptr<Node> child;
            if(curr->l.load() != nullptr){
                child = curr->l.load();
            }else{
                child = curr->r.load();
            }
//            Node::deleteNode(alloc,curr);

            child->p = parent;
            if(parent == nullptr){ //TODO можно заменить
                header.store(child);
            }else {
                if (comp(key, parent->value)) {
                    parent->l.store(child);
                } else {
                    parent->r.store(child);
                }
            }
        }else{
            // узел это лист, возможно надо удалить серого родителя
            if(parent == nullptr){
//                Node::deleteNode(alloc, curr);
                header = nullptr;
                return true;
            }
            if(parent->deleted) {
                std::shared_ptr<Node> child;
                if(comp(key,parent->value)){
                    child = parent->r.load(); //надо отдать gparent
                }else{
                    child = parent->l.load();
                }
//                Node::deleteNode(alloc, curr);
//                Node::deleteNode(alloc, parent);
                child->p.store(gparent);
                if(gparent == nullptr){ // TODO можно заменить
                    header.store(child);
                }else {
                    if (comp(key,gparent->value)) {
                        gparent->l.store(child);
                    } else {
                        gparent->r.store(child);
                    }
                }
            }else{
//                Node::deleteNode(alloc, curr);
                if (comp(key,parent->value)) {
                    parent->l.store(nullptr);
                } else {
                    parent->r.store(nullptr);
                }
            }
        }
        return true; // узел был, а в результате операции удален
    }
};
#endif //BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
