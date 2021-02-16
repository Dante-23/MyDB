#include "avl_ops.cpp"

vector<int> index_attr;

void test3(string str){
    
}

void test2(){
    vector<char*> arr(2);
    arr[0] = arr[1] = new char[STRING];
    string str = "hello";
    strcpy(arr[0], "hello");
    strcpy(arr[1], "hello");
}

int main(){
    /**
     * schema creation syntax
     * 1 create schema database_name(attr type, attr type, ......)
     * 
     * insert syntax
     * 2 database_name attr1value attr2value attr3value ........
     * 
     * print all tuples syntax
     * 3 database_name
     * 
     * print meta and schema file contents syntax
     * 4 database_name
     * 
     * Delete syntax
     * 5 database_name index_of_the_tuple (0 based index)
     **/
    while(1){
        cout << "enter query: ";
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
            cout << "created schema file" << endl;
            Write_In_Schema_File(name, schema);
            int tupleSize = getTupleSize(schema);
            cout << "wrote schema in schema file" << endl;
            strcpy(name, (dbname + ".meta").c_str());
            create_file(name);
            cout << "created metadata file" << endl;
            Initialize_Meta_File(name, tupleSize, dbname);
            cout << "initialized metadata file" << endl;
            tupleSize = Read_Tuple_Size(name);
            cout << "tupleSize: " << tupleSize << endl;
            strcpy(name, (dbname + ".db").c_str());
            create_file(name);
            cout << "created database file" << endl;
            strcpy(name, (dbname + ".avl").c_str());
            create_file(name);


            Initialize_AVLindex(name);


            cout << "created avl index file" << endl;
            strcpy(name, (dbname + ".btree").c_str());
            create_file(name);
            cout << "created B-Tree index file" << endl;
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
            cout << "Tuple: " << endl;
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
            cout << "Last tuple address: " << Read_Last_Tuple_Address(meta) << endl;
            cout << "Database size: " << Read_Database_Size(meta) << endl;
            cout << "Number of Tuples: " << Read_Num_Tuples(meta) << endl;
            cout << "Tuple Size: " << Read_Tuple_Size(meta) << endl;
            cout << "Database name: " << Read_DatabaseName(meta) << endl;
            strcpy(meta, (dbname + ".schema").c_str());
            cout << "Schema: " << endl;
            vector<pair<string,string>> schema = Read_Schema_File(meta);
            for(pair<string,string> p: schema){
                cout << p.first << " " << p.second << endl;
            }
        }
        else if(query[0] == '5'){
            int fsp = query.find(' '), ssp = query.find(' ', fsp + 1);
            string dbname = query.substr(fsp + 1, ssp - fsp - 1);
            int tupleNum = stoi(query.substr(ssp + 1, query.size() - ssp));
            // cout << dbname << " " << tupleNum << endl;
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(name);
            strcpy(name, (dbname + ".db").c_str());
            Delete_Data_Tuple(name, meta, schema, tupleNum, Read_Tuple_Size(meta));
        }
        else if(query[0] == '6'){
            char* name = (char*) malloc(STRING * sizeof(char));
            strcpy(name, "Student.avl");
            test1(name);
        }
        else if(query[0] == '7'){
            cout << "Enter key: " << endl;
            int key;
            cin >> key;
            char* name = (char*) malloc(STRING * sizeof(char));
            strcpy(name, "Student.avl");
            int root = Read_4Bytes_Address(name, 4);
            root = Delete_INT_AVLindex(name, root, -1, key);
            Update_Root_Address(name, root);
        }
        else if(query[0] == '8'){
            cout << "Inorder" << endl;
            char* name = (char*) malloc(STRING * sizeof(char));
            strcpy(name, "Student.avl");
            inorder(name, Read_4Bytes_Address(name, 4));
        }
        else if(query[0] == '9'){
            test2();
        }
    }
}