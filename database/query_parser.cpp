#include <iostream>
#include <string.h>
#include <vector>
using namespace std;

#define CREATE_SCHEMA 1

const string TYPE1 = "int";
const string TYPE2 = "string";
const string TYPE3 = "bool";

int getQueryType(string query){
    return 1;
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