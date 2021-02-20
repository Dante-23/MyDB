#include "disk_ops.cpp"
using namespace std;

/**
 * 4 bytes insert address, 4 bytes root address
 **/
int Initialize_AVLindex(char* name){
    int fd = open(name, O_WRONLY);
    int num = 2 * NODEPOINTER;
    write(fd, (void*)&num, NODEPOINTER);
    lseek(fd, NODEPOINTER, SEEK_SET);
    num = -1;
    write(fd, (void*)&num, NODEPOINTER);
    close(fd);
    return 1;
}

int Read_4Bytes_Address(char* name, int offset){
    int fd = open(name, O_RDONLY);
    lseek(fd, offset, SEEK_SET);
    void* buffer = malloc(NODEPOINTER);
    read(fd, buffer, NODEPOINTER);
    int *p = (int*)buffer;
    int data = *p;
    // cout << "data read from AVLindex: " << data << endl;
    free(buffer);
    close(fd);
    return data;
}

int Update_Insert_Address(char* name, int newAddress){
    int fd = open(name, O_WRONLY);
    ssize_t bytes = write(fd, (void*)&newAddress, NODEPOINTER);
    close(fd);
    return 1;
}

int Update_Root_Address(char* name, int newAddress){
    int fd = open(name, O_WRONLY);
    lseek(fd, 4, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&newAddress, NODEPOINTER);
    close(fd);
    return 1;
}

char* Read_32Bytes(char* name, int offset){
    int fd = open(name, O_RDONLY);
    lseek(fd, offset, SEEK_SET);
    char* res = (char*) malloc(STRING * sizeof(char));
    // cout << res << endl;
    read(fd, (void*)res, STRING);
    close(fd);
    return res;
}

int Write_AVLNODE(char* name, int nodeNum, AVLNODE* node){
    int fd = open(name, O_WRONLY);
    int offset = nodeNum;
    ssize_t bytes;
    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->intkey, NODEPOINTER);
    // cout << bytes << endl;
    offset += NODEPOINTER;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)node->stringkey, STRING);
    offset += STRING;
    // cout << bytes << endl;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->parent, NODEPOINTER);
    offset += NODEPOINTER;
    // cout << bytes << endl;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->left, NODEPOINTER);
    offset += NODEPOINTER;
    // cout << bytes << endl;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->right, NODEPOINTER);
    offset += NODEPOINTER;
    // cout << bytes << endl;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->size, NODEPOINTER);
    offset += NODEPOINTER;
    // cout << bytes << endl;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->index, NODEPOINTER);
    offset += NODEPOINTER;
    // cout << bytes << endl;

    lseek(fd, offset, SEEK_SET);
    bytes = write(fd, (void*)&node->height, NODEPOINTER);
    offset += NODEPOINTER;
    // cout << bytes << endl;

    for(auto i: node->blocks){
        int val = i;
        lseek(fd, offset, SEEK_SET);
        bytes = write(fd, (void*)&val, NODEPOINTER);
        offset += NODEPOINTER;
        // cout << bytes << endl;
    }
    for(int i = 0; i < (17 - node->blocks.size()); i++){
        lseek(fd, offset, SEEK_SET);
        int val = -1;
        bytes = write(fd, (void*)&val, NODEPOINTER);
        offset += NODEPOINTER;
        // cout << bytes << endl;
    }
    close(fd);
    return 1;
}

AVLNODE* Read_AVLNODE(char* name, int nodeNum, int index){
    int offset = nodeNum;
    AVLNODE* node = new AVLNODE();
    node->intkey = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    node->stringkey = Read_32Bytes(name, offset);
    offset += STRING;

    node->parent = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    node->left = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    node->right = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    node->size = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    node->index = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    node->height = Read_4Bytes_Address(name, offset);
    offset += NODEPOINTER;

    set<int> blocks;

    for(int i = 0; i < node->size; i++){
        int val = Read_4Bytes_Address(name, offset);
        offset += NODEPOINTER;
        // cout << "here" << endl;
        blocks.insert(val);
        // node->blocks.insert(val);
        // cout << "here" << endl;
    }
    node->blocks = blocks;

    return node;
}

int getHeight(char* name, int nodeNum){
    if(nodeNum == -1) return -1;
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    int height = node->height;
    delete node;
    return height;
    // int left = 0, right = 0;
    // if(node->left != -1){
    //     AVLNODE* leftNode = Read_AVLNODE(name, node->left, 0);
    //     left = leftNode->height + 1;
    //     free(leftNode);
    // }
    // if(node->right != -1){
    //     AVLNODE* rightNode = Read_AVLNODE(name, node->right, 0);
    //     right = rightNode->height + 1;
    // }
    // return max(left, right);
    return 1;
}

int RotateRight(char* name, int nodeNum){
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    AVLNODE* left = Read_AVLNODE(name, node->left, 0);
    int leftAddr = node->left;
    left->parent = node->parent;
    node->parent = node->left;
    node->left = left->right;
    left->right = nodeNum;
    node->height = max(getHeight(name, node->right) + 1, getHeight(name, node->left) + 1);
    left->height = max(node->height + 1, getHeight(name, left->left) + 1);
    Write_AVLNODE(name, nodeNum, node);
    Write_AVLNODE(name, leftAddr, left);
    int nodeAddress = node->parent;
    delete node;
    delete left;
    return nodeAddress;
}

int RotateLeft(char* name, int nodeNum){
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    AVLNODE* right = Read_AVLNODE(name, node->right, 0);
    int rightAddr = node->right;
    right->parent = node->parent;
    node->parent = node->right;
    node->right = right->left;
    right->left = nodeNum;
    node->height = max(getHeight(name, node->right) + 1, getHeight(name, node->left) + 1);
    right->height = max(getHeight(name, right->right) + 1, node->height + 1);
    Write_AVLNODE(name, nodeNum, node);
    Write_AVLNODE(name, rightAddr, right);
    int nodeAddress = node->parent;
    delete node;
    delete right;
    return nodeAddress;
}

int BalanceTree(char* name, int nodeNum){
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    int left = getHeight(name, node->left) + 1,
    right = getHeight(name, node->right) + 1;
    // cout << "left - right: " << left - right << endl;
    int resAddr = nodeNum;
    if(left - right > 1){
        // left
        AVLNODE* leftNode = Read_AVLNODE(name, node->left, 0);
        left = getHeight(name, leftNode->left) + 1;
        right = getHeight(name, leftNode->right) + 1;
        if(left >= right){
            // left left case
            resAddr = RotateRight(name, nodeNum);
        }
        else{
            node->left = RotateLeft(name, node->left);
            resAddr = RotateRight(name, nodeNum);
        }
    }
    else if(right - left > 1){
        // right
        AVLNODE* rightNode = Read_AVLNODE(name, node->right, 0);
        left = getHeight(name, rightNode->left) + 1;
        right = getHeight(name, rightNode->right) + 1;
        if(left >= right){
            node->right = RotateRight(name, node->right);
            resAddr = RotateLeft(name, nodeNum);
        }
        else{
            resAddr = RotateLeft(name, nodeNum);
        }
    }
    // Write_AVLNODE(name, nodeNum, node);
    // Write_AVLNODE(name, node->left, left);
    // Write_AVLNODE(name, node->right, right);
    return resAddr;
}

int Insert_INT_AVLindex(char* name, int nodeNum, int parent, int value, int block_pointer){
    // cout << "nodeNum: " << nodeNum << endl;
    if(nodeNum == -1){
        set<int> blocks;
        blocks.insert(block_pointer);
        AVLNODE* node = GetAVLNODE(value, name, parent, -1, -1, 1, 0, blocks);
        int insertNum = Read_4Bytes_Address(name, 0);
        Write_AVLNODE(name, insertNum, node);
        // cout << "here" << endl;
        // cout << "insertNum: " << insertNum << endl;
        Update_Insert_Address(name, insertNum + AVLNODESIZE);
        // cout << "Updated to: " << insertNum + AVLNODESIZE << endl;
        delete node;
        return insertNum;
    }
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    if(node->intkey == value){
        // cout << "present" << endl;
        node->size++;
        node->blocks.insert(block_pointer);
    }
    else if(value < node->intkey){
        int left = Insert_INT_AVLindex(name, node->left, nodeNum, value, block_pointer);
        node->left = left;
        AVLNODE* leftNode = Read_AVLNODE(name, left, 0);
        // node->height = max(node->height, leftNode->height + 1);
        node->height = max(getHeight(name, node->left) + 1, getHeight(name, node->right) + 1);
        delete leftNode;
    }
    else{
        int right = Insert_INT_AVLindex(name, node->right, nodeNum, value, block_pointer);
        node->right = right;
        AVLNODE* rightNode = Read_AVLNODE(name, right, 0);
        // node->height = max(node->height, rightNode->height + 1);
        node->height = max(getHeight(name, node->left) + 1, getHeight(name, node->right) + 1);
        delete rightNode;
    }
    Write_AVLNODE(name, nodeNum, node);
    nodeNum = BalanceTree(name, nodeNum);
    delete node;
    return nodeNum;
}

int Search_INT_AVLindex(char* name, int nodeNum, int parent, int value){
    // cout << "nodeNum: " << nodeNum << endl;
    if(nodeNum == -1) return -1;
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    // cout << "node->intkey: " << node->intkey << endl;
    int res = -1;
    if(node->intkey == value){
        res = nodeNum;
    }
    else if(value < node->intkey){
        res = Search_INT_AVLindex(name, node->left, nodeNum, value);
    }
    else{
        res = Search_INT_AVLindex(name, node->right, nodeNum, value);
    }
    delete node;
    return res;
}

int GetMaximum_INT_AVLindex(char* name, int nodeNum){
    if(nodeNum == -1) return -1;
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    int right = node->right;
    delete node;
    if(right == -1) return nodeNum;
    else return GetMaximum_INT_AVLindex(name, right);
}

int GetMinimum_INT_AVLindex(char* name, int nodeNum){
    if(nodeNum == -1) return -1;
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    int left = node->left;
    delete node;
    if(left == -1) return nodeNum;
    else return GetMinimum_INT_AVLindex(name, left);
}

int Delete_INT_AVLindex(char* name, int nodeNum, int parent, int value){
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    if(node->intkey == value){
        int inpre = GetMaximum_INT_AVLindex(name, node->left);
        if(inpre == -1){
            int insucc = GetMinimum_INT_AVLindex(name, node->right);
            if(insucc == -1){
                // this is a leaf
                return -1;
                // if(parent != -1){
                //     AVLNODE* par = Read_AVLNODE(name, parent, 0);
                //     if(par->left == nodeNum) par->left = -1;
                //     else par->right = -1;
                //     Write_AVLNODE(name, parent, par);
                    
                // }
            }
            else{
                AVLNODE* insuccnode = Read_AVLNODE(name, insucc, 0);
                insuccnode->right = Delete_INT_AVLindex(name, node->right, nodeNum, insuccnode->intkey);
                insuccnode->left = node->left;
                insuccnode->parent = node->parent;
                insuccnode->height = max(getHeight(name, insuccnode->left) + 1, 
                                getHeight(name, insuccnode->right) + 1);
                Write_AVLNODE(name, nodeNum, insuccnode);
            }
        }
        else{
            AVLNODE* inprenode = Read_AVLNODE(name, inpre, 0);
            inprenode->left = Delete_INT_AVLindex(name, node->left, nodeNum, inprenode->intkey);
            inprenode->right = node->right;
            inprenode->parent = node->parent;
            inprenode->height = max(getHeight(name, inprenode->left) + 1, 
                                getHeight(name, inprenode->right) + 1);
            Write_AVLNODE(name, nodeNum, inprenode);
        }
    }
    else if(value < node->intkey){
        node->left = Delete_INT_AVLindex(name, node->left, nodeNum, value);
        node->height = max(getHeight(name, node->left) + 1, 
                                getHeight(name, node->right) + 1);
        Write_AVLNODE(name, nodeNum, node);
    }
    else{
        node->right = Delete_INT_AVLindex(name, node->right, nodeNum, value);
        node->height = max(getHeight(name, node->left) + 1, 
                                getHeight(name, node->right) + 1);
        Write_AVLNODE(name, nodeNum, node);
    }
    delete node;
    node = Read_AVLNODE(name, nodeNum, 0);
    Write_AVLNODE(name, nodeNum, node);
    nodeNum = BalanceTree(name, nodeNum);
    delete node;
    return nodeNum;
}

void print_AVLnode(AVLNODE* node){
    cout << "intkey: " << node->intkey << endl;
    // cout << "stringkey: " << node->stringkey << endl;
    // cout << "parent: " << node->parent << endl;
    // cout << "left: " << node->left << endl;
    // cout << "right: " << node->right << endl;
    // cout << "size: " << node->size << endl;
    // cout << "index: " << node->index << endl;
    cout << "height: " << node->height << endl;
    // cout << "blocks" << endl;
    for(auto i: node->blocks){
        cout << i << " ";
    }
    cout << endl;
}

void inorder(char* name, int nodeNum){
    if(nodeNum == -1) return;
    AVLNODE* node = Read_AVLNODE(name, nodeNum, 0);
    inorder(name, node->left);
    // cout << node->intkey << endl;
    print_AVLnode(node);
    inorder(name, node->right);
}

void test1(char* name){
    // set<int> a;
    // a.insert(1);
    // a.insert(2);
    // // char* name = (char*) malloc(STRING * sizeof(char));
    // // strcpy(name, "Student.avl");
    // // test1(name);
    // AVLNODE* node = GetAVLNODE(1, name, -1, -1, -1, 2, 0, a);
    // // cout << node->stringkey << endl;
    // // Initialize_AVLindex(name);
    // int nodeNum = Read_4Bytes_Address(name, 0);
    // cout << nodeNum << endl;
    // // Write_AVLNODE(name, nodeNum, node);
    // cout << "here" << endl;
    // // free(node);
    // node = Read_AVLNODE(name, nodeNum, 0);
    // cout << "here" << endl;
    // // cout << node->index << endl;
    // print_AVLnode(node);
    // int root = Read_4Bytes_Address(name, 4);
    // inorder(name, root);
    while(1){
        // cout << "meta" << endl;
        // cout << Read_4Bytes_Address(name, 0) << " " << Read_4Bytes_Address(name, 4) << endl;
        // cout << "reading root" << endl;
        int root = Read_4Bytes_Address(name, 4);
        // cout << "root: " << root << endl;
        cout << "Enter value block_pointer: " << endl;
        int value, block_pointer;
        cin >> value >> block_pointer;
        cout << "here" << endl;
        root = Insert_INT_AVLindex(name, root, -1, value, block_pointer);
        cout << "root: " << root << endl;
        Update_Root_Address(name, root);
        cout << "inorder" << endl;
        inorder(name, root);
        break;
    }
}

// int main(){
    // // set<int> a;
    // // a.insert(1);
    // // a.insert(2);
    // char* name = (char*) malloc(STRING * sizeof(char));
    // strcpy(name, "Student.avl");
    // test1(name);
    // // AVLNODE* node = GetAVLNODE(1, name, -1, -1, -1, 2, 0, a);
    // // // cout << node->stringkey << endl;
    // // // Initialize_AVLindex(name);
    // // int nodeNum = Read_4Bytes_Address(name, 0);
    // // cout << nodeNum << endl;
    // // Write_AVLNODE(name, nodeNum, node);
    // // cout << "here" << endl;
    // // // free(node);
    // // node = Read_AVLNODE(name, nodeNum, 0);
    // // cout << "here" << endl;
    // // cout << node->index << endl;
// }