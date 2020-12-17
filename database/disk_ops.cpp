#include <stdio.h>
#include <fcntl.h> 
#include <errno.h>
#include <unistd.h>
#include <variant>
#include <string>
#include "query_parser.cpp"
using namespace std;

#define SCHEMA_ATTRIBUTE_SIZE 16
#define SCHEMA_TYPE_SIZE 8

#define META_TUPLE_ADDRESS 4
#define META_DATABASE_SIZE 4
#define META_NUM_TUPLES 4
#define META_TUPLE_SIZE 4

#define INTEGER 8
#define BOOL 1
#define STRING 32

extern int errno;

struct AttributeNode{
    int num;
    bool b;
    string str;
    int index;
};

AttributeNode* getAttributeNode(int num, bool b, string str, int index){
    cout << "in" << endl;
    AttributeNode* node = (AttributeNode*) malloc(sizeof(AttributeNode));
    cout << "in" << endl;
    node->index = index;
    cout << "in" << endl;
    if(index == 0) node->num = num;
    else if(index == 1) node->b = b;
    else node->str.assign(str);
    cout << "in" << endl;
    return node;
}

int create_file(char* name){
    // int fd = open(name, O_CREAT);
    int fd = creat(name, 0777);
    if(fd == -1){
        cout << "unable to create" << endl;
        close(fd);
        return -1;
    }
    else{
        close(fd);
        return 1;
    }
}

int Initialize_Meta_File(char* name){
    int fd = open(name, O_WRONLY);
    int offset = 0, nor = 0;

    lseek(fd, offset, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&nor, META_TUPLE_ADDRESS);
    offset += META_TUPLE_ADDRESS;

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&nor, META_DATABASE_SIZE);
    offset += META_DATABASE_SIZE;

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&nor, META_NUM_TUPLES);
    offset += META_NUM_TUPLES;

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&nor, META_TUPLE_SIZE);
    offset += META_TUPLE_SIZE;

    close(fd);
    return 1;
}

int Read_Last_Tuple_Address(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

int Update_Last_Tuple_Address(char* name, int address){
    int fd = open(name, O_WRONLY);
    ssize_t bytes = write(fd, (void*)&address, META_TUPLE_ADDRESS);
    close(fd);
    return 1;
}

int Read_Database_Size(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    lseek(fd, 4, SEEK_SET);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

int Update_Database_Size(char* name, int newsize){
    int fd = open(name, O_WRONLY);
    lseek(fd, 4, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&newsize, META_DATABASE_SIZE);
    close(fd);
    return 1;
}

int Read_Num_Tuples(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    lseek(fd, 8, SEEK_SET);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

int Update_Num_Tuples(char* name, int tuples){
    int fd = open(name, O_WRONLY);
    lseek(fd, 8, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&tuples, META_NUM_TUPLES);
    close(fd);
    return 1;
}

int Read_Tuple_Size(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    lseek(fd, 12, SEEK_SET);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

int Write_In_Schema_File(char* name, vector<pair<string,string>> schema){
    int fd = open(name, O_WRONLY);
    char* attr = (char*) malloc(SCHEMA_ATTRIBUTE_SIZE * sizeof(char));
    char* type = (char*) malloc(SCHEMA_TYPE_SIZE * sizeof(char));
    int offset = 0;
    for(pair<string,string> p: schema){
        strcpy(attr, p.first.c_str());
        strcpy(type, p.second.c_str());
        lseek(fd, offset, SEEK_SET);
        ssize_t written1 = write(fd, (void*)attr, SCHEMA_ATTRIBUTE_SIZE);
        offset += SCHEMA_ATTRIBUTE_SIZE;
        lseek(fd, offset, SEEK_SET);
        ssize_t written2 = write(fd, (void*)type, SCHEMA_TYPE_SIZE);
        offset += SCHEMA_TYPE_SIZE;
    }
    close(fd);
    free(attr);
    free(type);
    return 1;
}

vector<pair<string,string>> Read_Schema_File(char* name){
    vector<pair<string,string>> schema;
    int fd = open(name, O_RDONLY);
    char* attr = (char*) malloc(SCHEMA_ATTRIBUTE_SIZE * sizeof(char));
    char* type = (char*) malloc(SCHEMA_TYPE_SIZE * sizeof(char));
    int offset = 0;
    while(1){
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes = read(fd, (void*)attr, SCHEMA_ATTRIBUTE_SIZE);
        if(bytes == 0) break;
        offset += SCHEMA_ATTRIBUTE_SIZE;
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes1 = read(fd, (void*)type, SCHEMA_TYPE_SIZE);
        if(bytes1 == 0) break;
        offset += SCHEMA_TYPE_SIZE;
        schema.push_back({ attr, type });
    }
    free(attr);
    free(type);
    close(fd);
    return schema;
}

int Write_In_Data_File(char* name, char* meta, vector<AttributeNode*> tuple, int tuplesize){
    int last_address = Read_Last_Tuple_Address(meta);
    int fd = open(name, O_WRONLY), offset = last_address;
    char* temp = (char*) malloc(STRING * sizeof(char));
    for(AttributeNode* node: tuple){
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes;
        if(node->index == 0){
            bytes = write(fd, (void*)&node->num, INTEGER);
            offset += INTEGER;
        }
        else if(node->index == 1){
            bytes = write(fd, (void*)&node->b, BOOL);
            offset += BOOL;
        }
        else{
            strcpy(temp, node->str.c_str());
            bytes = write(fd, (void*)temp, STRING);
            offset += STRING;
        }
    }
    free(temp);
    close(fd);
    Update_Last_Tuple_Address(meta, last_address + tuplesize);
    Update_Database_Size(meta, Read_Database_Size(meta) + tuplesize);
    Update_Num_Tuples(meta, Read_Num_Tuples(meta) + 1);
    return 1;
}

vector<AttributeNode*> Read_Data_File(char* name, vector<pair<string,string>> schema, int tupleNum, int tuplesize){
    vector<AttributeNode*> tuple(tuplesize);
    int index = 0, offset = tupleNum * tuplesize;
    int fd = open(name, O_RDONLY);
    void* num = malloc(INTEGER), *b = malloc(BOOL);
    char* temp = (char*) malloc(STRING * sizeof(char));
    for(pair<string,string> sch: schema){
        lseek(fd, offset, SEEK_SET);
        if(sch.second == TYPE1){
            read(fd, num, INTEGER);
            int* p = (int*)num;
            AttributeNode* node = getAttributeNode(*p, false, "", 0);
            tuple[index++] = node;
            offset += INTEGER;
        }
        else if(sch.second == TYPE2){
            read(fd, (void*)temp, STRING);
            string str;
            copy(str.begin(), str.end(), temp);
            tuple[index++] = getAttributeNode(1, false, str, 2);
            offset += STRING;
        }
        else{
            read(fd, b, BOOL);
            bool* p = (bool*)b;
            tuple[index++] = getAttributeNode(1, *p, "", 1);
            offset += BOOL;
        }
    }
    close(fd);
    free(num);
    free(b);
    free(temp);
    return tuple;
}

void f(){
    // char* schname = (char*) malloc(STRING * sizeof(char));
    // strcpy(schname, "test.schema");
    // create_file(schname);
    // string query;
    // getline(cin, query);
    // vector<pair<string,string>> schema = parse_schema_DDL(query);
    // Write_In_Schema_File(schname, schema);
    // strcpy(schname, "test.meta");
    // create_file(schname);
    // Initialize_Meta_File(schname);
    // strcpy(schname, "test.db");
    // create_file(schname);
    // string str, temp = "fa";
    // str.assign(temp);
    // cout << str << endl;
    char* fname = (char*) malloc(STRING * sizeof(char));
    strcpy(fname, "test.schema");
    // cout << "here" << endl;
    vector<pair<string,string>> schema = Read_Schema_File(fname);
    cout << "schema: " << endl;
    for(pair<string,string> p: schema) cout << p.first << " " << p.second << endl;
    strcpy(fname, "test.db");
    char* schname = (char*) malloc(STRING * sizeof(char));
    strcpy(schname, "test.meta");
    int size, i = 0;
    cout << "enter number of tuples: ";
    cin >> size;
    while(i < size){
        cout << "Enter tuple: " << endl;
        string name;
        int roll;
        bool b;
        cin >> name >> roll >> b;
        vector<AttributeNode*> tuple(3);
        cout << "here" << endl;
        tuple[0] = getAttributeNode(1, false, name, 2);
        cout << "here" << endl;
        tuple[1] = getAttributeNode(roll, false, "", 0);
        cout << "here" << endl;
        tuple[2] = getAttributeNode(1, b, "", 1);
        cout << "here" << endl;
        Write_In_Data_File(fname, schname, tuple, STRING + INTEGER + BOOL);
        for(int i = 0; i < 3; i++) free(tuple[i]);
    }
}

int main(){
    f();
    exit(0);
    cout << "done" << endl;
    string str;
    // getline(cin, str);
    string file = "test.schema";
    char* name = (char*) malloc(file.size() * sizeof(char));
    strcpy(name, "test.schema");
    // vector<pair<string,string>> schema = parse_schema_DDL(str);
    // Write_In_Schema_File(name, schema);
    vector<pair<string,string>> schemas = Read_Schema_File(name);
    cout << "here" << endl;
    for(pair<string,string> p: schemas){
        cout << p.first << " " << p.second << endl;
    }
    // return 0;
}