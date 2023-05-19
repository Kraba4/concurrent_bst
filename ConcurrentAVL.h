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
#define mmo std::memory_order_relaxed

template <typename Key, typename Compare = std::less<Key>, typename Alloc = std::allocator<Key>>
class ConcurrentAVL{
    struct Node{
        using Node_allocator = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
        using Alloc_traits = typename std::allocator_traits<Node_allocator>;
        Key value;
        int height;
        std::atomic<Node*> l;
        std::atomic<Node*> r;
        std::atomic<Node*> p;

        //блокировка, личная у каждого узла
        cds::sync::spin_lock<cds::backoff::LockDefault> mtx;

        /* информация для partially external tree, если true, то вершина считается вспомогательной для навигации
         * логически в контейнере ее нет*/
        bool routing;

        /* метка того, что вершина удалена(недостижима в дереве). Нужно потому, что пока один поток
         * нашел вершину и собирается начать работать с ней, другой поток может удалить ее из дерева*/
        bool deleted;

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

        Node(const Key& value, Node* parent): l(), r(), p(parent),
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
public:
    ConcurrentAVL(Alloc alloc = Alloc()): alloc(alloc){}
    ~ConcurrentAVL(){
        header.store(nullptr);
    }
private:
    bool equiv(const Key& a, const Key& b) const{
        return !comp(a, b) && !comp(b, a);
    }

    //Функция поиска узла, его родителя и родителя родителя по ключу.
    std::tuple<guard, guard, guard, Node*, Node*, Node*>
//    std::tuple<Node*, Node*, Node*>
    search(const std::atomic<Node*>& root, const Key& key) {
        guard gparent_grd;
        guard parent_grd;
        guard curr_grd;
        Node* gparent = nullptr;
        Node* parent = nullptr;
        Node* curr = curr_grd.protect(root); //защищаем указатель с помощью hazard_pointer
//        Node* curr = root.load(mmo);
        while(curr){
            if(equiv(curr->value,key)){ //найден ключ
                break;
            }

            // спускаемся по дереву
            gparent = std::move(parent);
            parent = std::move(curr);
            gparent_grd.copy(parent_grd);
            parent_grd.copy(curr_grd);
            if(comp(key,parent->value)){
                curr = curr_grd.protect(parent->l);
//                curr = parent->l.load(mmo);
            }else{
                curr = curr_grd.protect(parent->r);
//                curr= parent->r.load(mmo);
            }
        }
        return {std::move(gparent_grd), std::move(parent_grd), std::move(curr_grd), std::move(gparent), std::move(parent), std::move(curr)};
//          return {std::move(gparent), std::move(parent), std::move(curr)};
    }

public:
    bool contains(const Key& key){
        // lock-free операция, никаких блокировок нет
        if(header.load(std::memory_order_relaxed) == nullptr){
            return false;
        }
        auto [gp_grd, p_grd, c_grd, gparent, parent, curr] = search(header, key);
//        auto [gparent, parent, curr] = search(header, key);
        return curr && !curr->routing; //узел найден и он не помечен как удаленный, тогда true
    }
    bool insert_new_node(Node* parent, const Key& key){

        {
            std::lock_guard lock(parent->mtx);
            if (parent->deleted) {//пока брали блокировку, кто-то удалил узел
                return false;
            }
            if (comp(key, parent->value)) { // смотрим куда надо поставить Node(key)
                if (parent->l.load(std::memory_order_relaxed) !=
                    nullptr) { //пока брали блокировку, кто-то занял нужное место
                    return false;
                }
                //так как взята блокировка, никто не может изменить parent->l с момента проверки условия != nullptr
                parent->l.store(Node::createNode(alloc, key, parent), std::memory_order_relaxed);
            } else {
                if (parent->r.load(std::memory_order_relaxed) != nullptr) {
                    return false;
                }
                parent->r.store(Node::createNode(alloc, key, parent), std::memory_order_relaxed);
            }
        }
        fix(parent);
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
            if (header.load(std::memory_order_relaxed) == nullptr) {
                std::lock_guard lock(null_header_mtx);
                if(header.load(std::memory_order_relaxed) != nullptr){ //пока брали блокировку, кто-то занял header
                    continue;
                }
                header.store(Node::createNode(alloc, key, nullptr), std::memory_order_relaxed);
                break;
            }
            auto [gp_grd, p_grd, c_grd, gparent, parent, curr] = search(header, key);
//            auto [gparent, parent, curr] = search(header, key);
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
        if(curr->l.load(mmo) != nullptr){
            childGuard.protect(curr->l);
            child = curr->l.load(std::memory_order_relaxed);
        }else{
            childGuard.protect(curr->r);
            child = curr->r.load(std::memory_order_relaxed);
        }
        curr->deleted = true;
        if(!parent){ //TODO можно заменить
            header.store(child, std::memory_order_release); //потенциальная ошибка todo
            child->p.store(header);
            cds::gc::HP::retire<NodeDisposer>(curr);
        }else {
            if (comp(key, parent->value)) {
                parent->l.store(child, std::memory_order_release);
                child->p.store(parent);
                cds::gc::HP::retire<NodeDisposer>(curr);
            } else {
                parent->r.store(child, std::memory_order_release);
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
//                parent->l.store(nullptr, std::memory_order_release);
            }else{
                childGuard.protect(parent->l);
                child = parent->l.load(std::memory_order_relaxed);
//                parent->r.store(nullptr, std::memory_order_release);
            }

            if(!gparent){ // TODO можно заменить
                header.store(child, std::memory_order_release);
                child->p.store(header);
            }else {
                if (comp(key,gparent->value)) {
                    gparent->l.store(child, std::memory_order_release);
                    child->p.store(gparent);
                } else {
                    gparent->r.store(child, std::memory_order_release);
                    child->p.store(gparent);
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
        while (true) {
            auto [gp_grd, p_grd, c_grd, gparent, parent, curr] = search(header, key);
//            auto [gparent, parent, curr] = search(header, key);
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
    std::tuple<int, int, int, int> take_node_params(Node* curr){
        Node* l = curr->l.load(mmo);
        Node* r = curr->r.load(mmo);
        int l_height = l == nullptr? 0 : l->height;
        int r_height = r == nullptr? 0 : r->height;
        int new_curr_height = 1 + std::max(l_height, r_height);
        int balance_factor = l_height - r_height;
        return {l_height, r_height, new_curr_height, balance_factor};
    }

    Node* fix_height(Node* curr){
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
        return curr->p.load(mmo); // теперь надо чинить родителя
    }
    void internal_right_rotate(Node* curr, Node* parent, Node* curr_l_child){
        assert(curr != parent);
        assert(curr != curr_l_child);
        assert(parent!=curr_l_child);
        //теперь parent указывает на левого ребенка curr
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
        //передаем curr правого ребенка от curr_l_child
        Node* curr_l_child_r = curr_l_child->r.load(mmo);
        curr->l.store(curr_l_child_r);
        if(curr_l_child_r!=nullptr) {
            curr_l_child_r->p.store(curr);
            assert(curr_l_child_r != curr_l_child_r->p);
            assert(curr_l_child_r != parent);
        }
        //curr опускается ниже и делается правым ребенком curr_l_child
        curr->p.store(curr_l_child);
        curr_l_child->r.store(curr);

        curr->recalculate_height();
        curr_l_child->recalculate_height();
        assert(curr != curr->r.load());
        assert(curr_l_child != curr_l_child->r.load());
        assert(curr != curr->l.load());
        assert(curr_l_child != curr_l_child->l.load());
    }

    void internal_left_rotate(Node* curr, Node* parent, Node* curr_r_child){
        //теперь parent указывает на правого ребенка curr
        assert(curr != parent);
        assert(curr != curr_r_child);
        assert(parent != curr_r_child);
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
        //передаем curr левого ребенка от curr_r_child
        Node* curr_r_child_l = curr_r_child->l.load(mmo);
        curr->r.store(curr_r_child_l);
        if(curr_r_child_l!=nullptr) {
            curr_r_child_l->p.store(curr);
            assert(curr_r_child_l != curr_r_child_l->p);
            assert(curr_r_child_l != parent);
        }
        //curr опускается ниже и делается правым ребенком curr_l_child
        curr->p.store(curr_r_child);
        curr_r_child->l.store(curr);

        curr->recalculate_height();
        curr_r_child->recalculate_height();

        assert(curr != curr->r.load());
        assert(curr_r_child != curr_r_child->r.load());
        assert(curr != curr->l.load());
        assert(curr_r_child != curr_r_child->l.load());

    }
    Node* small_right_rotate(Node* curr, Node* parent, Node* curr_l_child){
        internal_right_rotate(curr, parent, curr_l_child);
        return parent;
    }
    Node* big_right_rotate(Node* curr, Node* parent, Node* curr_l_child, Node* curr_l_child_r){
        //тут уже должна быть взята блокировка на curr и parent и curr_l_child
//        int l_balance_factor = curr_l_child == nullptr ? 0 : curr_l_child->get_balance_factor();
//        if(l_balance_factor>=0){//пока брали блокировку кто-то сбалансировал детей curr_l_child
//            return small_right_rotate(curr, parent, curr_l_child);
//        }

//        Node* curr_l_child_r = curr_l_child->r.load();
        internal_left_rotate(curr_l_child, curr, curr_l_child_r);
        internal_right_rotate(curr, parent, curr_l_child_r);

        return parent;
    }
    Node* right_rotate(Node* curr, Node* parent, Node* curr_l_child){
        //тут уже должна быть взята блокировка на curr и parent
        int l_balance_factor = curr_l_child == nullptr ? 0 : curr_l_child->get_balance_factor();
        if(l_balance_factor >= 0){
            return small_right_rotate(curr, parent, curr_l_child); //curr и parent уже взяты в lock
        }else{
            Node* curr_l_child_r = curr_l_child->r.load(mmo);
            if(curr_l_child_r) {
                std::lock_guard lock(curr_l_child_r->mtx);
                return big_right_rotate(curr, parent, curr_l_child, curr_l_child_r);
            }else{
                return big_right_rotate(curr, parent, curr_l_child, curr_l_child_r);
            }
        }
    }
    Node* small_left_rotate(Node* curr, Node* parent, Node* curr_r_child){
        internal_left_rotate(curr, parent, curr_r_child);
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
        internal_right_rotate(curr_r_child, curr, curr_r_child_l);
        internal_left_rotate(curr, parent, curr_r_child_l);
        return parent;
    }
    Node* left_rotate(Node* curr, Node* parent,  Node* curr_r_child){
        //тут уже должна быть взята блокировка на curr и parent
//        Node* curr_r_child = curr->r.load();
        int r_balance_factor = curr_r_child == nullptr ? 0 : curr_r_child->get_balance_factor();
        if(r_balance_factor <= 0){
            return small_left_rotate(curr, parent, curr_r_child); //curr и parent уже взяты в lock
        }else{
            Node* curr_r_child_l = curr_r_child->l.load(mmo);
            if(curr_r_child_l) {
                std::lock_guard lock(curr_r_child_l->mtx);
                return big_left_rotate(curr, parent,curr_r_child, curr_r_child_l);
            }else{
                return big_left_rotate(curr, parent,curr_r_child, curr_r_child_l);
            }
        }
    }
    Node* fix_balance(Node* curr, Node* parent){
        //нужно чтобы уже был взят блок на curr и parent(если он не nullptr)
        if(curr->deleted){//пока брали блокировку curr удален
            return nullptr;//заканчиваем чинить дерево
        }
        if(parent && (parent->deleted || curr->p.load(mmo) != parent) ){ //пока брали блокировку с родителем стало плохо
            return curr;                                 //повторить операцию
        }
        auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);
        if(balance_factor > 1){
            Node* curr_l_child = curr->l.load(mmo);
            if(curr_l_child) {
                std::lock_guard lock(curr_l_child->mtx);
                return right_rotate(curr, parent, curr_l_child);
            }else{
                return right_rotate(curr, parent, curr_l_child);
            }
        }else if(balance_factor < -1){
            Node* curr_r_child = curr->r.load(mmo);
            if(curr_r_child){
                std::lock_guard lock(curr_r_child->mtx);
                return left_rotate(curr, parent, curr_r_child);
            }else {
                return left_rotate(curr, parent, curr_r_child);
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
        while(curr != nullptr && !curr->deleted){
            auto [l_height, r_height, new_curr_height, balance_factor] = take_node_params(curr);

            if(balance_factor>=-1 && balance_factor <=1){//с высотами детей все в порядке
                if(new_curr_height == curr->height){
                    //кто-то все починил
                    return;
                }
                curr = fix_height(curr); // корректируем свою высоту и получаем следующий узел для балансировки
            }else{
                Node* parent = curr->p.load(mmo);
                if(parent!=nullptr) {
                    std::lock_guard lock1(parent->mtx);
                    std::lock_guard lock2(curr->mtx);
                    curr = fix_balance(curr, parent);
                }else{
                    std::lock_guard lock(curr->mtx);
                    curr = fix_balance(curr, parent);
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
