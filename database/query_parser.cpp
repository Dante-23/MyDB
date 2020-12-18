#include <iostream>
#include <string.h>
#include <vector>
using namespace std;

#define CREATE_SCHEMA 1

#define INTEGER 4
#define BOOL 1
#define STRING 32

const string TYPE1 = "int";
const string TYPE2 = "string";
const string TYPE3 = "bool";

struct AttributeNode{
    int num;
    bool b;
    char* str;
    int index;
};

AttributeNode* getAttributeNode(int num, bool b, string str, int index){
    AttributeNode* node = (AttributeNode*) malloc(sizeof(AttributeNode));
    node->str = (char*) malloc(STRING * sizeof(char));
    node->index = index;
    if(index == 0) node->num = num;
    else if(index == 1) node->b = b;
    else{
        strcpy(node->str, str.c_str());
    }
    return node;
}

int getTupleSize(vector<pair<string,string>> schema){
    int tot = 0;
    for(pair<string,string> p: schema){
        if(p.second == TYPE1) tot += INTEGER;
        else if(p.second == TYPE2) tot += STRING;
        else tot += BOOL;
    }
    return tot;
}

vector<AttributeNode*> getTuple(vector<pair<string,string>> schema, string query){
    vector<AttributeNode*> tuple(schema.size());
    int index = 0, i = 0;
    string str = "";
    query += " ";
    while(i < query.size()){
        if(query[i] == ' ' && str != ""){
            if(schema[index].second == TYPE1){
                tuple[index++] = getAttributeNode(stoi(str), false, "", 0);
            }
            else if(schema[index].second == TYPE2){
                tuple[index++] = getAttributeNode(1, false, str, 2);                
            }
            else
                tuple[index++] = getAttributeNode(1, stoi(str), "", 1);
            str = "";
        }
        else str += query[i];
        i++;
    }
    return tuple;
}

string getDatabaseName(string query){
    int sp = query.find("schema") + 7,
    obrace = query.find("(");
    return query.substr(sp, obrace - sp);
}

vector<pair<string,string>> parse_schema_DDL(string query){
    vector<pair<string,string>> schema;
    int obrace = query.find('('), cbrace = query.find(')');
    string attr = "", type = "";
    int i = obrace + 1, index = 0;
    while(i < cbrace){
        if(query[i] == ' '){
            if(attr.size() > 0){
                schema.push_back({ attr, "" });
                attr = "";
            }
            i++;
            continue;
        }
        else if(query[i] == ','){
            if(attr.size() > 0){
                schema[index].second = attr;
                index++;
                attr = "";
            }
            i++;
            continue;
        }
        attr += query[i];
        i++;
    }
    if(attr.size() > 0) schema[index].second = attr;
    return schema;
}

// int main(){
//     string query;
//     getline(cin, query);
//     vector<pair<string,string>> arr = parse_schema_DDL(query);
//     for(pair<string,string> p: arr) cout << p.first << " " << p.second << endl;
// }