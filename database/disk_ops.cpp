#include <stdio.h>
#include <fcntl.h> 
#include <errno.h>
#include <unistd.h>
#include <variant>
#include <string>
#include "query_parser.cpp"
using namespace std;

#define SCHEMA_ATTRIBUTE_SIZE 16 // each schema attribute like name, roll etc will acquire this much size in schema file
#define SCHEMA_TYPE_SIZE 8 // each schema attribute type like int, string etc will acquire this much size in schema file

#define META_TUPLE_ADDRESS 4 // The address at which new tuple will be inserted. Takes 4 bytes in meta file.
#define META_DATABASE_SIZE 4 // Size of database. Takes 4 bytes in meta file
#define META_NUM_TUPLES 4 // Number of tuples in database. Take 4 bytes in meta file
#define META_TUPLE_SIZE 4 // Size of each tuple. Take 4 bytes in meta file

extern int errno; // for printing errors

// takes a name as input and makes a file
// used to create to .schema .meta and .db files when first initializing a database
int create_file(char* name){
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

int Delete_file(char* name){
    return remove(name);
}

/**
 * After creating .meta file, it is initialized
 * meta file contains five information as of now
 * Last tuple address = address to insert new row
 * Database size
 * Number of tuples in database
 * Tuple size
 * Database name --> which takes 32 bytes in meta file
 * parameters:
 * char* name = .meta file
 * dbname = name of database
 **/
int Initialize_Meta_File(char* name, int tupleSize, string dbname){
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
    write(fd, (void*)&tupleSize, META_TUPLE_SIZE);
    offset += META_TUPLE_SIZE;

    char* temp = (char*) malloc(STRING * sizeof(char));
    strcpy(temp, dbname.c_str());

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)temp, STRING);
    offset += STRING;

    write(fd, (void*)&nor, INTEGER);
    offset += INTEGER;

    close(fd);
    free(temp);
    return 1;
}

// Given a meta file, read the number of indexes
// which are their in the database
int Read_Num_Index(char* name){
    int offset = 32 + 16;
    int fd = open(name, O_RDONLY);
    lseek(fd, offset, SEEK_SET);
    void* buf = malloc(INTEGER);
    read(fd, buf, INTEGER);
    int* p = (int*)buf;
    int res = *p;
    free(buf);
    close(fd);
    return res;
}

// Given the meta file, update the number of 
// indexes for the particular database
int Update_Num_Index(char* name, int indexNum){
    int offset = 32 + 16;
    int fd = open(name, O_WRONLY);
    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&indexNum, INTEGER);
    close(fd);
    return 1;
}

// Write the positions of attributes on which index is created
// in the given meta file
int Write_Index(char* name, vector<int> arr){
    int size = (int)arr.size();
    Update_Num_Index(name, size);
    int offset = 32 + 20;
    int fd = open(name, O_WRONLY);
    lseek(fd, offset, SEEK_SET);
    for(int i: arr){
        write(fd, (void*)&i, INTEGER);
        offset += INTEGER;
        lseek(fd, offset, SEEK_SET);
    }
    close(fd);
    return 1;
}

// Read the positions of attributes on which index is created
// in the given meta file
vector<int> Read_Index(char* name){
    int size = Read_Num_Index(name);
    vector<int> arr;
    int offset = 32 + 20;
    int fd = open(name, O_RDONLY);
    lseek(fd, offset, SEEK_SET);
    void* buf = malloc(INTEGER);
    while(size--){
        read(fd, buf, INTEGER);
        int *p = (int*)buf;
        arr.push_back(*p);
        offset += INTEGER;
        lseek(fd, offset, SEEK_SET);
    }
    free(buf);
    close(fd);
    return arr;
}

/**
 * Read the address at which new tuple will be inserted in data file
 * char* name = .meta file
 **/
int Read_Last_Tuple_Address(char* name){
    int fd = open(name, O_RDONLY);
    // void* buffer = malloc(4);
    int p;
    ssize_t bytes = read(fd, &p, sizeof(p));
    // int p = *((int*)buffer);
    close(fd);
    // free(buffer);
    return p;
}

/**
 * Updates the address at which new tuple will be inserted in data file
 * char* name = .meta file
 * address = address at which tuple will be inserted
 **/
int Update_Last_Tuple_Address(char* name, int address){
    int fd = open(name, O_WRONLY);
    ssize_t bytes = write(fd, &address, META_TUPLE_ADDRESS);
    close(fd);
    return 1;
}

/**
 * Read database size
 * char* name = .meta file
 **/
int Read_Database_Size(char* name){
    int fd = open(name, O_RDONLY);
    // void* buffer = malloc(4);
    lseek(fd, 4, SEEK_SET);
    int p;
    ssize_t bytes = read(fd, &p, META_DATABASE_SIZE);
    // int p = *((int*)buffer);
    close(fd);
    return p;
}

/**
 * Updates database size
 * char* name = .meta file
 **/
int Update_Database_Size(char* name, int newsize){
    int fd = open(name, O_WRONLY);
    lseek(fd, 4, SEEK_SET);
    ssize_t bytes = write(fd, &newsize, META_DATABASE_SIZE);
    close(fd);
    return 1;
}

/**
 * reads number of tuples in database
 * char* name = .meta file
 **/
int Read_Num_Tuples(char* name){
    int fd = open(name, O_RDONLY);
    // void* buffer = malloc(4);
    lseek(fd, 8, SEEK_SET);
    int p;
    ssize_t bytes = read(fd, &p, META_TUPLE_ADDRESS);
    // int p = *((int*)buffer);
    close(fd);
    return p;
}

/**
 * updates number of tuples in database
 * char* name = .meta file
 **/
int Update_Num_Tuples(char* name, int tuples){
    int fd = open(name, O_WRONLY);
    lseek(fd, 8, SEEK_SET);
    ssize_t bytes = write(fd, &tuples, META_NUM_TUPLES);
    close(fd);
    return 1;
}

/**
 * Reads size of each tuple
 * char* name = .meta file
 **/
int Read_Tuple_Size(char* name){
    int fd = open(name, O_RDONLY);
    // void* buffer = malloc(4);
    lseek(fd, 12, SEEK_SET);
    int p;
    ssize_t bytes = read(fd, &p, META_TUPLE_ADDRESS);
    // int p = *((int*)buffer);
    close(fd);
    return p;
}

string Read_DatabaseName(char* name){
    int fd = open(name, O_RDONLY);
    char* buffer = (char*) malloc(STRING * sizeof(char));
    lseek(fd, 16, SEEK_SET);
    ssize_t bytes = read(fd, (void*)buffer, STRING);
    close(fd);
    return buffer;
}

/**
 * Schema file contains contains schema details in pair
 * First = name of attribute
 * Second = Data type of attribute
 * schema = contains schema info in pairs like (name, string), (roll, int) etc
 * name = .schema file
 **/
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

/**
 * reads the schema from schema file and returns array of pairs.
 * Example (name, string) (roll, int) etc
 * char* name = .schema file
 **/
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

/**
 * writes data in .db file and updates .meta file
 * tuple = array containing tuple info in the same order as in schema
 * char* name = .db file
 * char* meta = .meta file
 * tuplesize = size of each tuple in this database.
 **/
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
    int dbsize = Read_Database_Size(meta);
    int numtuple = Read_Num_Tuples(meta);
//     cout << "TupleSize: " << tuplesize << endl;
//     cout << "Old last tuple address: " << last_address << endl;
//     cout << "Old database size: " << dbsize << endl;
//     cout << "Old number of tuples: " << numtuple << endl;
//     cout << "New last tuple address: " << last_address + tuplesize << endl;
//     cout << "New database size: " << dbsize + tuplesize << endl;
//     cout << "New number of tuples: " << numtuple + 1 << endl;
    Update_Last_Tuple_Address(meta, last_address + tuplesize);
    Update_Database_Size(meta, dbsize + tuplesize);
    Update_Num_Tuples(meta, numtuple + 1);
    dbsize = Read_Database_Size(meta);
    numtuple = Read_Num_Tuples(meta);
    int ts = Read_Tuple_Size(meta);
//     cout << "TupleSize: " << ts << endl;
//     cout << "Old last tuple address: " << last_address << endl;
//     cout << "Old database size: " << dbsize << endl;
//     cout << "Old number of tuples: " << numtuple << endl;
//     cout << "Old last tuple address: " << last_address + tuplesize << endl;
//     cout << "Old database size: " << Read_Database_Size(meta) + tuplesize << endl;
//     cout << "Old number of tuples: " << Read_Num_Tuples(meta) + 1 << endl;
    return last_address;
}

/**
 * This functions writes data at a specified index like writing it at index 2 or 3 etc.
 * This is used in deletion and updation.
 * When a tuple is deleted, the last tuple is copied to the deleted tuple position or index and .meta file is updated.
 * tuple = same as above
 * tupleNum = index to write tuple
 * tuplesize = size of each tuple
 * char* name = .db file
 **/
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
    close(fd);
    return 1;
}

/**
 * Reads the tuple at the specified position (tupleNum)
 * schema = schema in pair form like (name, string), (roll, int) etc
 * tupleSize = size of each tuple
 * tupleNum = positon or index of the tuple to read
 * char* name = .db file
 **/
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

/**
 * Deleted tuple at the specified position or index (tupleNum)
 * copies the last tuple to the deleted position or index
 * .meta file is updated
 * char* meta = .meta file
 * char* name = .db file
 * tupleNum = position or index of tuple to be deleted
 * tupleSize = size of each tuple
 **/
int Delete_Data_Tuple(char* name, char* meta, vector<pair<string,string>> schema, int tupleNum, int tupleSize){
    vector<AttributeNode*> tuple = Read_Data_File(name, schema, Read_Num_Tuples(meta) - 1, tupleSize);
    Write_At_Location(name, tuple, tupleNum, tupleSize);
    Update_Last_Tuple_Address(meta, Read_Last_Tuple_Address(meta) - tupleSize);
    Update_Database_Size(meta, Read_Database_Size(meta) - tupleSize);
    Update_Num_Tuples(meta, Read_Num_Tuples(meta) - 1);
    return 1;
}