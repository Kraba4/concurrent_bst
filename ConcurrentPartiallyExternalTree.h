//
// Created by HONOR on 24.04.2023.
//

#ifndef BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
#define BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
//using Alloc_traits = typename std::allocator_traits<Node_allocator>;
#include <functional>
#include <memory>
#include <mutex>
template <typename Key, typename Compare = std::less<Key>,
          typename Alloc = std::allocator<Key>>
class ConcurrentPartiallyExternalTree{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        Key value;
        int height;
        std::atomic<std::shared_ptr<Node>> l;
        std::atomic<std::shared_ptr<Node>> r;
        std::mutex mtx;

        /* информация для partially external, если true, то вершина считается вспомогательной для навигации
         * логически в контейнере ее нет*/
        bool routing;

        /* метка того, что вершина удалена(недостижима в дереве). Нужно потому, что пока один поток
         * нашел вершину и собирается начать работать с ней, другой поток может удалить ее из дерева*/
        bool deleted;


        Node(std::shared_ptr<Node> parent, const Key& value): l(nullptr), r(nullptr),
                                        value(value), height(0), routing(false), deleted(false){}
        ~Node(){
//            std::cout << "destroy " << value << std::endl;
            l.store(nullptr);
            r.store(nullptr);
        }
        template<typename ...Args>
        static std::shared_ptr<Node> createNode(Node_allocator& alloc, Args&&... args){
            return std::allocate_shared<Node>(alloc,std::forward<Args>(args)...);
        }
//        static void deleteNode(Node_allocator& alloc, std::shared_ptr<Node> node){
//            Alloc_traits::destroy(alloc, node);
//            Alloc_traits::deallocate(alloc, node, 1);
//        }
    };
    typename Node::Node_allocator alloc;
    Compare comp;
    std::atomic<std::shared_ptr<Node>> header;
    std::mutex null_header_mtx;
public:
    int retries=0;
    ConcurrentPartiallyExternalTree(Alloc alloc = Alloc()): alloc(alloc){}
    ~ConcurrentPartiallyExternalTree(){
//        std::cout << "tree" << std::endl;
        header.store(nullptr);
    }
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
//        std::cout << "[ ";
        while(curr != nullptr){
//            std::cout << curr->value << ' ';
            if(equiv(curr->value,key)){
                break;
            }

            // спускаемся по дереву
            gparent = parent;
            parent = curr;
            if(comp(key,curr->value)){
                curr = curr->l.load(std::memory_order_relaxed);
            }else{
                curr = curr->r.load(std::memory_order_relaxed);
            }
        }
//        std::cout << "] ";
        return {std::move(gparent), std::move(parent), std::move(curr)};
    }

public:
    bool contains(const Key& key) const{
        // lock-free операция, никаких блокировок нет
//        std::cout << "\ncontains " << key << ' ';
        auto [gparent, parent, curr] = search(header, key);
//        std::cout <<  bool(curr!=nullptr && !curr->routing);
        return curr!=nullptr && !curr->routing; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert_new_node(std::shared_ptr<Node>& parent, const Key& key){
        std::lock_guard<std::mutex> lock(parent->mtx);
        if(parent->deleted) {//пока брали блокировку, кто-то удалил узел
            return false;
        }
        if (comp(key,parent->value)) { // смотрим куда надо поставить Node(key)
            if(parent->l.load() != nullptr) { //пока брали блокировку, кто-то занял нужное место
                return false;
            }
            //так как взята блокировка, никто не может изменить parent->l с момента проверки условия != nullptr
            parent->l.store(Node::createNode(alloc, parent, key));
        } else {
            if(parent->r.load() != nullptr) {
                return false;
            }
            parent->r.store(Node::createNode(alloc, parent, key));
        }
        return true;
    }
    bool insert_existing_node(std::shared_ptr<Node>& curr, const Key& key){
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
                header.store(Node::createNode(alloc, nullptr, key));
                break;
            }
            auto [gparent, parent, curr] = search(header, key);

            bool result;
            if (curr == nullptr) {//узла с нужным ключем нет в дереве
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
    bool remove_with_two_child(std::shared_ptr<Node>& curr){
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
    bool remove_with_one_child(std::shared_ptr<Node>& curr, std::shared_ptr<Node>& parent, const Key& key){
        std::mutex dummy_parent_mtx;
        auto lock = (parent!= nullptr) ? std::scoped_lock(parent->mtx, curr->mtx):
                                         std::scoped_lock(dummy_parent_mtx, curr->mtx);
        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }
        if(parent!= nullptr && parent->deleted){
            return false;//родитель удален, если привяжем ребенка к нему, он потеряется. Нужно начать операцию заново
        }
        if((curr->l.load() != nullptr) + (curr->r.load() != nullptr) != 1){
            return false; //пока брали блокировку, вершина изменила вид
        }
        // надо удалить curr и отдать его ребенка parent
        std::shared_ptr<Node> child;
        if(curr->l.load() != nullptr){
            child = curr->l.load();
        }else{
            child = curr->r.load();
        }
        curr->deleted = true;
        if(parent == nullptr){ //TODO можно заменить
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
    bool remove_with_zero_child(std::shared_ptr<Node>& curr, std::shared_ptr<Node>& parent,
                              std::shared_ptr<Node>& gparent, const Key& key){
        //на случай если gparent == nullptr parent==nullptr,
        //ведь надо взять блокировку только у существующих узлов
        //dummy_mtx для того, чтобы взять блокировку на несуществующий mutex(пустого mutex не существует)
        std::mutex dummy_gparent_mtx;
        std::mutex dummy_parent_mtx;
        auto lock =
            (gparent!= nullptr && parent != nullptr)? std::scoped_lock(gparent->mtx,parent->mtx,curr->mtx):
            (parent!= nullptr)?                       std::scoped_lock(dummy_gparent_mtx, parent->mtx, curr->mtx):
                                                      std::scoped_lock(dummy_gparent_mtx, dummy_parent_mtx, curr->mtx);

        if(curr->deleted){
            return true;//вершина удалена, кто-то удалил за нас
        }
        if(gparent!= nullptr&& gparent->deleted){
            return false;//удален предок, нужно начать заново
        }
        if(parent!= nullptr&&parent->deleted){
            return false;
        }
        if((curr->l.load() != nullptr) + (curr->r.load() != nullptr) != 0){
            return false;//пока брали блокировку, вершина изменила вид
        }
        // узел это лист, возможно надо удалить серого родителя
        if(parent == nullptr){
            curr->deleted = true;
            header.store(nullptr);
            return true;
        }
        if(parent->routing) {
            std::shared_ptr<Node> child;
            if(comp(key,parent->value)){
                child = parent->r.load(std::memory_order_relaxed); //надо отдать gparent
            }else{
                child = parent->l.load(std::memory_order_relaxed);
            }
            curr->deleted = true;
            parent->deleted = true;
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
            curr->deleted = true;
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
            if (curr == nullptr || curr->routing) {
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
            }else{
                retries++;
            }
        }
        return true; // узел был, а в результате операции удален
    }
};
#endif //BACHELOR_CONCURRENTPARTIALLYEXTERNALTREE_H
