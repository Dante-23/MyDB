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

extern int errno;

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

int Initialize_Meta_File(char* name, string dbname){
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

    char* temp = (char*) malloc(STRING * sizeof(char));
    strcpy(temp, dbname.c_str());

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)temp, STRING);
    offset += STRING;

    close(fd);
    free(temp);
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

string Read_DatabaseName(char* name){
    int fd = open(name, O_RDONLY);
    char* buffer = (char*) malloc(STRING * sizeof(char));
    lseek(fd, 16, SEEK_SET);
    ssize_t bytes = read(fd, (void*)buffer, STRING);
    return buffer;
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
            bytes = write(fd, (void*)node->str, STRING);
            offset += STRING;
        }
    }
    // free(temp);
    close(fd);
    Update_Last_Tuple_Address(meta, last_address + tuplesize);
    Update_Database_Size(meta, Read_Database_Size(meta) + tuplesize);
    Update_Num_Tuples(meta, Read_Num_Tuples(meta) + 1);
    return 1;
}

int Write_At_Location(char* name, vector<AttributeNode*> tuple, int tupleNum, int tupleSize){
    int fd = open(name, O_WRONLY);
    int offset = tupleNum * tupleSize;
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
            bytes = write(fd, (void*)node->str, STRING);
            offset += STRING;
        }
    }
    return 1;
}

vector<AttributeNode*> Read_Data_File(char* name, vector<pair<string,string>> schema, int tupleNum, int tuplesize){
    vector<AttributeNode*> tuple((int)schema.size());
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
            tuple[index++] = getAttributeNode(1, false, temp, 2);
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

int Delete_Data_Tuple(char* name, char* meta, vector<pair<string,string>> schema, int tupleNum, int tupleSize){
    vector<AttributeNode*> tuple = Read_Data_File(name, schema, Read_Num_Tuples(meta) - 1, tupleSize);
    Write_At_Location(name, tuple, tupleNum, tupleSize);
    Update_Last_Tuple_Address(meta, Read_Last_Tuple_Address(meta) - tupleSize);
    Update_Database_Size(meta, Read_Database_Size(meta) - tupleSize);
    Update_Num_Tuples(meta, Read_Num_Tuples(meta) - 1);
    return 1;
}

// int Update_Data_Tuple(char* name, )

void f(){
    char* schname = (char*) malloc(STRING * sizeof(char));
    strcpy(schname, "test.schema");
    create_file(schname);
    string query;
    getline(cin, query);
    vector<pair<string,string>> schema = parse_schema_DDL(query);
    Write_In_Schema_File(schname, schema);
    strcpy(schname, "test.meta");
    create_file(schname);
    Initialize_Meta_File(schname, getDatabaseName(query));
    strcpy(schname, "test.db");
    create_file(schname);

    // string str, temp = "fa";
    // str.assign(temp);
    // cout << str << endl;
    // char* fname = (char*) malloc(STRING * sizeof(char));
    // strcpy(fname, "test.schema");
    // // cout << "here" << endl;
    // vector<pair<string,string>> schema = Read_Schema_File(fname);
    // cout << "schema: " << endl;
    // for(pair<string,string> p: schema) cout << p.first << " " << p.second << endl;
    // strcpy(fname, "test.db");
    // char* schname = (char*) malloc(STRING * sizeof(char));
    // strcpy(schname, "test.meta");
    // cout << "before" << endl;
    // cout << Read_Last_Tuple_Address(schname) << endl;
    // cout << Read_Database_Size(schname) << endl;
    // cout << Read_Num_Tuples(schname) << endl;
    // int size, i = 0;
    // cout << "enter number of tuples: ";
    // cin >> size;
    // while(i < size){
    //     cout << "Enter tuple: " << endl;
    //     string name;
    //     int roll, bo;
    //     bool b = false;
    //     cin >> name >> roll >> bo;
    //     if(bo) b = true;
    //     vector<AttributeNode*> tuple(3);
    //     cout << "here" << endl;
    //     tuple[0] = getAttributeNode(1, false, name, 2);
    //     cout << "here" << endl;
    //     tuple[1] = getAttributeNode(roll, false, "", 0);
    //     cout << "here" << endl;
    //     tuple[2] = getAttributeNode(1, b, "", 1);
    //     cout << "here" << endl;
    //     Write_In_Data_File(fname, schname, tuple, STRING + INTEGER + BOOL);
    //     for(int j = 0; j < 3; j++){
    //         if(tuple[j]->index == 2) free(tuple[j]->str);
    //         free(tuple[j]);
    //     }
    //     i++;
    // }
    strcpy(schname, "test.meta");
    cout << "after" << endl;
    cout << Read_Last_Tuple_Address(schname) << endl;
    cout << Read_Database_Size(schname) << endl;
    cout << Read_Num_Tuples(schname) << endl;
    cout << Read_DatabaseName(schname) << endl;
}

void g(){
    char* name = (char*) malloc(STRING * sizeof(char));
    strcpy(name, "test.db");
    char* sch = (char*) malloc(STRING * sizeof(char));
    strcpy(sch, "test.schema");
    vector<pair<string,string>> schema = Read_Schema_File(sch);
    cout << "schema" << endl;
    for(pair<string,string> p: schema){
        cout << p.first << " " << p.second << endl;
    }
    int ts = STRING + INTEGER + BOOL;
    strcpy(sch, "test.meta");
    Delete_Data_Tuple(name, sch, schema, 1, STRING + INTEGER + BOOL);
    int size = Read_Num_Tuples(sch);
    for(int i = 0; i < size; i++){
        vector<AttributeNode*> tuple = Read_Data_File(name, schema, i, ts);
        for(AttributeNode* node: tuple){
            if(node->index == 0) cout << node->num << " ";
            else if(node->index == 1) cout << node->b << " ";
            else cout << node->str << " ";
        }
        cout << endl;
    }
}

int main(){
    while(1){
        cout << "enter: ";
        string query;
        getline(cin, query);
        if(query[0] == '1'){
            int sp = query.find(' ');
            query = query.substr(sp + 1, query.size() - sp);
            vector<pair<string,string>> schema = parse_schema_DDL(query);
            string dbname = getDatabaseName(query);
            char* name = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            create_file(name);
            Write_In_Schema_File(name, schema);
            strcpy(name, (dbname + ".meta").c_str());
            create_file(name);
            Initialize_Meta_File(name, dbname);
            strcpy(name, (dbname + ".db").c_str());
            create_file(name);
            free(name);
        }
        else if(query[0] == '2'){
            int sp = query.find(' ') + 1;
            string dbname = query.substr(sp, query.find(' ', sp) - sp);
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            vector<AttributeNode*> tuple = getTuple(Read_Schema_File(name), query.substr(query.find(' ', sp) + 1, query.size() - query.find(' ', sp)));
            int tupleSize = getTupleSize(Read_Schema_File(name));
            strcpy(name, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            Write_In_Data_File(name, meta, tuple, tupleSize);
            free(name);
            free(meta);
        }
        else if(query[0] == '3'){
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            string dbname = query.substr(query.find(' ') + 1, query.size() - query.find(' '));
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(name, (dbname + ".db").c_str());
            int size = Read_Num_Tuples(meta);
            strcpy(meta, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(meta);
            for(int i = 0; i < size; i++){
                vector<AttributeNode*> tuple = Read_Data_File(name, schema, i, getTupleSize(schema));
                for(AttributeNode* node: tuple){
                    if(node->index == 0) cout << node->num << " ";
                    else if(node->index == 1) cout << node->b << " ";
                    else cout << node->str << " ";
                }
                cout << endl;
            }
            free(name);
            free(meta);
        }
        else if(query[0] == '4'){
            string dbname = query.substr(query.find(' ') + 1, query.size() - query.find(' '));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(meta, (dbname + ".meta").c_str());
            cout << Read_Last_Tuple_Address(meta) << endl;
            cout << Read_Database_Size(meta) << endl;
            cout << Read_Num_Tuples(meta) << endl;
            strcpy(meta, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(meta);
            for(pair<string,string> p: schema){
                cout << p.first << " " << p.second << endl;
            }
        }
        else{

        }
    }
}