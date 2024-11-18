#include <iostream>
#include <algorithm>
#include <sstream>
#include <string>
#include "json.hpp"
#include <fstream>
#include <filesystem>
#include <iomanip>
#include "Utils.h"

using namespace std;
using json = nlohmann::json;
namespace fs = std::filesystem;

int main() {
	
    setlocale(LC_ALL, "ru"); 

    cout << "        ~Список команд~" << endl;
    cout << "SELECT             ---   sel" << endl;
    cout << "INSERT INTO        ---   ins" << endl;
    cout << "DELETE FROM        ---   del" << endl;
    cout << "CREATE DIRECTORY   ---   cdir" << endl;
    cout << "HELP               ---   help" << endl;
    cout << "EXIT               ---   exit" << endl << endl;
    string command;
    while (true) {
        cout << "> ";
        getline(cin, command);

        if (command == "exit") {
            return false;
        }
        if (command == "help") {
            cout << endl;
            cout << "        ~Список команд~" << endl;
            cout << "SELECT             ---   sel" << endl;
            cout << "INSERT INTO        ---   ins" << endl;
            cout << "DELETE FROM        ---   del" << endl;
            cout << "CREATE DIRECTORY   ---   cdir" << endl;
            cout << "HELP               ---   help" << endl;
            cout << "EXIT               ---   exit" << endl << endl;
        }
        if (command == "cdir"){
            cout << endl;
            createSchemaStructure("schema.json");
            cout << endl;
        }
        if(command == "sel"){
            string schema = schemaName("schema.json");
            string input;
            getline(cin, input);
            lockTablesFromQuery(input, 1);
            RowNode* table = processSelectQuery(input, schema);
            cout << "WHERE ";
            string cond;
            getline(cin, cond);
            RowNode* result = selectFiltration(table, cond);
            cout << "Результат: " << endl;
            printTableSecond(result);
            lockTablesFromQuery(input, 0);
            freeOneTable(table);
            freeOneTable(result);
        }
        if (command == "del"){
            string schema = schemaName("schema.json");
            string input;
            getline(cin, input);
            lockTablesFromQuery(input, 1);
            string tableName = extractName(input);
            RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
            cout << "WHERE ";
            string cond;
            getline(cin, cond);
            addTableNames(table, tableName);
            addColumnNames(table);
            RowNode* result = deleteFrom(table, cond);
            cout << "Результат: " << endl;
            printTableSecond(result);
            convertToCSV(result, schema + "/" + tableName);
            lockTablesFromQuery(input, 0);
            freeOneTable(table);
            freeOneTable(result);
        }
        if (command == "ins"){
            int tuples = tuplesLimit("schema.json");
            string input;
            getline(cin, input);
            lockTablesFromQuery(input, 1);
            string tableName = extractName(input);
            cout << "VALUES ";
            string values;
            getline(cin, values);
            string listString[50];       
            parseValues(values, listString);
            RowNode* result = insertInto(nullptr, listString, tableName, tuples, "schema.json");
            lockTablesFromQuery(input, 0);
            freeOneTable(result);
        }
    }
    
	return 0;
}
