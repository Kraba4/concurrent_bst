//
// Created by HONOR on 08.05.2023.
//

#ifndef BACHELOR_OLD_H

//
// Created by HONOR on 24.04.2023.
//
#include <iostream>
#include <functional>
template <typename Key, typename Compare = std::less<Key>>
class ConcurrentPartiallyExternalTreeOld{
    struct Node{
        Key value;
        Node* l;
        Node* r;
        Node* p;
        int height;
        bool deleted;
        Node(Node* parent, const Key& value): p(parent), l(nullptr), r(nullptr),
                                              value(value), height(0), deleted(false){}
    };
    Node* header = nullptr;
    //Функция поиска узла, его родителя и родителя родителя по ключу.
    std::tuple<Node*, Node*, Node*> search(Node* root, const Key& key) const {
        Node* gparent = nullptr;
        Node* parent = nullptr;
        Node* curr = root;
//        std::cout << "[ ";
        while(curr != nullptr){
//            std::cout << curr->value << ' ';
            if(curr->value == key){
                break;
            }

            // спускаемся по дереву
            gparent = parent;
            parent = curr;
            if(key < curr->value){
                curr = curr->l;
            }else{
                curr = curr->r;
            }
        }
//        std::cout << "] ";
        return {gparent, parent, curr};
    }
public:
    bool contains(const Key& key) const{
//        std::cout << "\ncontains " << key << ' ';
        auto [gparent, parent, curr] = search(header, key);
//        std::cout <<  bool(curr!=nullptr && !curr->deleted);
        return curr!=nullptr && !curr->deleted; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert(const Key& key) {
//        std::cout << "\ninsert " << key << ' ';
        if(header == nullptr){
            header = new Node(nullptr, key);
            return true;
        }
        auto [gparent, parent, curr] = search(header, key);
        if(curr == nullptr){ //узла с нужным ключем нет в дереве
            if(key < parent->value){ // возвращаемся к родителю и ищем чем был curr(левым или правым ребенком)
                parent->l = new Node(parent, key);
            }else{
                parent->r = new Node(parent, key);
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
//        std::cout << "\nremove " << key << ' ';
        auto [gparent, parent, curr] = search(header, key);
        if(curr == nullptr || curr->deleted){
            return false; // узла нет в дереве, операция ни на что не повлияла
        }
        int n_children = (curr->l != nullptr) + (curr->r != nullptr);
        if(n_children == 2){
            // у вершины оба ребенка есть, надо только пометить его удаленным
            curr->deleted = true;
        }else if(n_children == 1){
            // надо удалить curr и отдать его ребенка parent
            Node* child;
            if(curr->l != nullptr){
                child = curr->l;
            }else{
                child = curr->r;
            }
            delete curr;

            child->p = parent;
            if(parent == nullptr){ //TODO можно заменить
                header = child;
            }else {
                if (key < parent->value) {
                    parent->l = child;
                } else {
                    parent->r = child;
                }
            }
        }else{
            // узел это лист, возможно надо удалить серого родителя
            if(parent == nullptr){
                delete curr;
                header = nullptr;
                return true;
            }
            if(parent->deleted) {
                Node* child;
                if(key < parent->value){
                    child = parent->r; //надо отдать gparent
                }else{
                    child = parent->l;
                }
                delete curr;
                delete parent;
                child->p = gparent;
                if(gparent == nullptr){ // TODO можно заменить
                    header = child;
                }else {
                    if (key < gparent->value) {
                        gparent->l = child;
                    } else {
                        gparent->r = child;
                    }
                }
            }else{
                delete curr;
                if (key < parent->value) {
                    parent->l = nullptr;
                } else {
                    parent->r = nullptr;
                }
            }
        }
        return true; // узел был, а в результате операции удален
    }
};
#define BACHELOR_OLD_H

#endif //BACHELOR_OLD_H
