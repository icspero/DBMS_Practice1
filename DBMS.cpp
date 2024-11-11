#include <iostream>
#include <algorithm>
#include <sstream>
#include <string>
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include <iomanip>

using namespace std;
using json = nlohmann::json;
namespace fs = std::filesystem;

struct Node {
	int numColumn;
	string name;
	string cell;
	Node* next;
};

struct RowNode {
	string name;
	Node* cell;
	RowNode* nextRow;
};

void freeOneTable(RowNode* table) {
    while (table != nullptr) {
        RowNode* tempRow = table;
        table = table->nextRow;

        Node* currNode = tempRow->cell;
        while (currNode != nullptr) {
            Node* tempNode = currNode;
            currNode = currNode->next;
            delete tempNode;
        }
        delete tempRow;
    }
}

void freeAllTables(RowNode** tables, int count) {
    for (int i = 0; i < count; ++i) {
        freeOneTable(tables[i]); 
    }
}

void printTableSecond(RowNode* table) {
    if (table == nullptr) return;

    Node* header = table->cell;
    int maxColumnWidths[50] = {0}; 
    int colCount = 0;

    while (header != nullptr) {
        maxColumnWidths[colCount] = header->name.length(); 
        header = header->next;
        colCount++;
    }

    RowNode* row = table->nextRow; 
    while (row != nullptr) {
        Node* cell = row->cell;
        int colIdx = 0;
        while (cell != nullptr && colIdx < colCount) {
            int width = cell->cell.length();
            if (width > maxColumnWidths[colIdx]) {
                maxColumnWidths[colIdx] = width;
            }
            cell = cell->next;
            colIdx++;
        }
        row = row->nextRow;
    }

    auto printSeparator = [&](bool isEnd) {
        for (int i = 0; i < colCount; i++) {
            cout << "+";
            for (int j = 0; j < maxColumnWidths[i] + 2; j++) {
                cout << "-";
            }
        }
        cout << "+\n";
        if (isEnd) return;
    };

    printSeparator(false);
    header = table->cell;
    cout << "|";
    for (int i = 0; i < colCount; i++) {
        cout << " " << setw(maxColumnWidths[i]) << left << header->name << " |";
        header = header->next;
    }
    cout << "\n";
    printSeparator(false);

    row = table->nextRow; 
    while (row != nullptr) {
        Node* cell = row->cell;
        cout << "|";
        for (int i = 0; i < colCount; i++) {
            if (cell != nullptr) {
                cout << " " << setw(maxColumnWidths[i]) << left << cell->cell << " |";
                cell = cell->next;
            } else {
                cout << " " << setw(maxColumnWidths[i]) << " " << " |"; 
            }
        }
        cout << "\n";
        if (row->nextRow != nullptr) {
            printSeparator(false); 
        }
        row = row->nextRow;
    }
    printSeparator(true);
}

int tuplesLimit(const string& configPath){

	ifstream configFile(configPath);

    json config;
    configFile >> config;
    configFile.close();

	int tuples_Limit = config["tuples_limit"];
	
	return tuples_Limit;
}

string schemaName(const string& configPath){

	ifstream configFile(configPath);

    json config;
    configFile >> config;
    configFile.close();

	string name = config["name"];
	
	return name;
}

void createSchemaStructure(const string& configPath) {
    
    ifstream configFile(configPath);
    if (!configFile.is_open()) {
        cerr << "Не удалось открыть файл конфигурации: " << configPath << endl;
        return;
    }

    json config;
    configFile >> config;
    configFile.close();

    string schemaName = config["name"];
    int tuplesLimit = config["tuples_limit"];
    auto structure = config["structure"];

    fs::create_directory(schemaName);

    for (auto& [tableName, columns] : structure.items()) {
        string tableDir = schemaName + "/" + tableName;

        fs::create_directory(tableDir);

        ofstream csvFile(tableDir + "/1.csv");
        if (!csvFile.is_open()) {
            cerr << "Не удалось создать файл для таблицы: " << tableName << endl;
            continue;
        }

        for (size_t i = 0; i < columns.size(); ++i) {
            csvFile << columns[i];
            if (i < columns.size() - 1) {
                csvFile << ",";
            }
        }
        csvFile << "\n";
        csvFile.close();
    }

    cout << "Директории и файлы успешно созданы для схемы: " << schemaName << endl;
}

void appendRow(RowNode*& tail, Node* rowHead) {
    RowNode* newRow = new RowNode;
    newRow->cell = rowHead;
    newRow->nextRow = nullptr;

    if (tail) {
        tail->nextRow = newRow;
    }
    tail = newRow;
}

RowNode* convertCSVToLinkedList(const string& directoryPath) {
    RowNode* head = nullptr;
    RowNode* tail = nullptr;
    bool isHeaderLoaded = false;
    string line;

    for (int i = 1; ; ++i) {
        string filePath = directoryPath + "/" + to_string(i) + ".csv";
        if (!fs::exists(filePath)) {
            break;
        }

        ifstream file(filePath);
        if (!file.is_open()) {
            cerr << "Не удалось открыть файл!" << filePath << endl;
            continue;
        }

        bool isFirstLine = true;
        while (getline(file, line)) {
            if (isFirstLine && isHeaderLoaded) {
                isFirstLine = false;
                continue;
            }

            istringstream ss(line);
            string cell;
            Node* rowHead = nullptr;
            Node* rowTail = nullptr;
            int colIndex = 0;

            while (getline(ss, cell, ',')) {
                if (!cell.empty() && cell.front() == '"' && cell.back() == '"') {
                    cell = cell.substr(1, cell.size() - 2);
                }

                Node* newNode = new Node;
                newNode->numColumn = colIndex++;
                newNode->cell = cell;
                newNode->next = nullptr;

                if (isFirstLine && !isHeaderLoaded) {
                    newNode->name = cell; 
                } else {
                    newNode->name = "колонка" + to_string(newNode->numColumn + 1);
                }

                if (rowHead == nullptr) {
                    rowHead = newNode;
                } else {
                    rowTail->next = newNode;
                }
                rowTail = newNode;
            }

            RowNode* newRow = new RowNode;
            newRow->cell = rowHead;
            newRow->nextRow = nullptr;

            if (isFirstLine && !isHeaderLoaded) {
                head = newRow;
                tail = head;
                isHeaderLoaded = true;
            } else {
                appendRow(tail, rowHead);
                if (head == nullptr) {
                    head = tail;
                }
            }

            isFirstLine = false;
        }

        file.close();
    }

    return head;
}

void convertToCSV(RowNode* table, const string& directory) {
    int tuples = tuplesLimit("schema.json");
    int rowCount = 0;
    int fileIndex = 1;
    RowNode* currentRow = table->nextRow; 

    Node* headerCell = table->cell;
    string headerLine;
    while (headerCell != nullptr) {
        headerLine += "" + headerCell->name + "";
        headerCell = headerCell->next;
        if (headerCell != nullptr) {
            headerLine += ",";
        }
    }

    while (currentRow != nullptr) {
        string filename = directory + "/" + to_string(fileIndex) + ".csv";
        ofstream csvFile(filename, ios::out | ios::trunc);

        if (!csvFile.is_open()) {
            cerr << "Не удалось открыть файл: " << filename << endl;
            return;
        }

        csvFile << headerLine << "\n";

        for (int i = 0; i < tuples && currentRow != nullptr; i++) {
            Node* currentCell = currentRow->cell;
            while (currentCell != nullptr) {
                csvFile << currentCell->cell;
                currentCell = currentCell->next;
                if (currentCell != nullptr) {
                    csvFile << ",";
                }
            }
            csvFile << "\n";
            currentRow = currentRow->nextRow;
            rowCount++;
        }

        csvFile.close();
        fileIndex++;
    }

    while (fs::exists(directory + "/" + to_string(fileIndex) + ".csv")) {
        fs::remove(directory + "/" + to_string(fileIndex) + ".csv");
        fileIndex++;
    }
}

int readPrimaryKey(const string& tablePath, const string& tableName) {
    ifstream pkFile(tablePath + "/" + tableName + "_pk_sequence");
    int primaryKey = 0;
    if (pkFile.is_open()) {
        pkFile >> primaryKey;
        pkFile.close();
    }
    return primaryKey;
}

void updatePrimaryKey(const string& tablePath, const string& tableName, int newPrimaryKey) {
    ofstream pkFile(tablePath + "/" + tableName + "_pk_sequence", ios::trunc);
    if (pkFile.is_open()) {
        pkFile << newPrimaryKey;
        pkFile.close();
    }
}

void appendRowToCSV(const string& filePath, const string listString[], int numCols) {
    ofstream csvFile(filePath, ios::app);
    if (csvFile.is_open()) {
        for (int i = 0; i < numCols; i++) {
            csvFile << listString[i];
            if (i < numCols - 1) csvFile << ",";
        }
        csvFile << "\n";
        csvFile.close();
    }
}

RowNode* insertInto(RowNode* table, string listString[], const string& tableName, int tuplesLimit, const string& configPath) {
    string schema = schemaName("schema.json");
    string tablePath;
    tablePath.append(schema).append("/").append(tableName);
    
	ifstream configFile(configPath);

	json config;
	configFile >> config;
	configFile.close();

	auto structure = config["structure"];

    if (!filesystem::exists(tablePath)) {
        cerr << "Директория " << tablePath << " не существует!\n";
        return nullptr; 
    }
    
    int primaryKey = readPrimaryKey(tablePath, tableName) + 1;
    if (primaryKey == 0) {
        cerr << "Не удалось прочитать или обновить значение первичного ключа для таблицы " << tableName << "!\n";
        return nullptr;
    }
    updatePrimaryKey(tablePath, tableName, primaryKey);

    listString[0] = to_string(primaryKey);

    RowNode* newRow = new RowNode;
    newRow->cell = nullptr;
    newRow->nextRow = nullptr;

    Node* currNode = nullptr;
    for (int i = 0; listString[i] != ""; i++) {
        Node* newNode = new Node;
        newNode->cell = listString[i];
        newNode->next = nullptr;

        if (newRow->cell == nullptr) {
            newRow->cell = newNode;
        } else {
            currNode->next = newNode;
        }
        currNode = newNode;
    }

    if (table == nullptr) {
        table = newRow;
    } else {
        RowNode* currRow = table;
        while (currRow->nextRow != nullptr) {
            currRow = currRow->nextRow;
        }
        currRow->nextRow = newRow;
    }

    int fileIndex = 1;
    string filePath = tablePath + "/" + to_string(fileIndex) + ".csv";
    while (filesystem::exists(filePath)) {
        int lineCount = 0;
        ifstream file(filePath);
        if (!file.is_open()) {
            cerr << "Не удалось открыть файл " << filePath << " для чтения!\n";
            return nullptr;
        }
        string line;
        while (getline(file, line)) ++lineCount;

        if (lineCount < tuplesLimit + 1) { 
            break;
        }
        fileIndex++;
        filePath = tablePath + "/" + to_string(fileIndex) + ".csv";
    }

    if (!filesystem::exists(filePath)) {
        ofstream newFile(filePath);
        if (!newFile.is_open()) {
            cerr << "Не удалось создать файл " << filePath << " для записи!\n";
            return nullptr;
        }
		for (auto& [tableName, columns] : structure.items()){
			for (size_t i = 0; i < columns.size(); ++i) {
                newFile << columns[i];
                if (i < columns.size() - 1) {
                    newFile << ",";
                }
            }
			newFile << "\n";
			newFile.close();
		}
    }

    int numCols = 0;
    while (listString[numCols] != "") numCols++;
    appendRowToCSV(filePath, listString, numCols);
    cout << "Строка успешно добавлена!\n";
    return table;
}

RowNode* selectFromTable(RowNode* table, const string& name, const string& cond) {
    
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;
    
    Node* lastHeaderCell = nullptr;
    
    Node* headerCell = table->cell;

    while (headerCell != nullptr) {
        Node* newHeaderCell = new Node;
        newHeaderCell->name = headerCell->name;
        newHeaderCell->cell = headerCell->cell;
        newHeaderCell->next = nullptr;

        if (headerRow->cell == nullptr) {
            headerRow->cell = newHeaderCell;
        } else {
            lastHeaderCell->next = newHeaderCell;
        }
        lastHeaderCell = newHeaderCell;
        headerCell = headerCell->next;
    }
    

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    int condNum = -1;
    Node* currCell = table->cell;
    for (int i = 1; currCell != nullptr; i++) {
        if (currCell->name == name) { 
            condNum = i;
            break;
        }
        currCell = currCell->next;
    }

    if (condNum == -1) {
        cout << "Колонка с именем " << name << " не найдена!" << endl;
        return nullptr;
    }

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        Node* cell = currentRow->cell;
        int cellIndex = 1;
        bool rowMatches = false;

        while (cell != nullptr) {
            if (cellIndex == condNum && cell->cell == cond) {
                rowMatches = true;
                break;
            }
            cell = cell->next;
            cellIndex++;
        }

        if (rowMatches) {
            RowNode* newRow = new RowNode;
            newRow->nextRow = nullptr;

            Node* newCell = nullptr;
            Node* newCellTail = nullptr;
            cell = currentRow->cell;

            while (cell != nullptr) {
                Node* copiedCell = new Node;
                copiedCell->name = cell->name;
                copiedCell->cell = cell->cell;
                copiedCell->next = nullptr;

                if (newCell == nullptr) {
                    newCell = copiedCell;
                    newCellTail = copiedCell;
                } else {
                    newCellTail->next = copiedCell;
                    newCellTail = newCellTail->next;
                }
                cell = cell->next;
            }
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;
    
    return headerRow;
}

RowNode* selectFromTables(RowNode* table, const string& name1, const string& cond1, const string& name2, const string& cond2){
    
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;
    
    Node* lastHeaderCell = nullptr;
    
    Node* headerCell = table->cell;

    while (headerCell != nullptr) {
        Node* newHeaderCell = new Node;
        newHeaderCell->name = headerCell->name;
        newHeaderCell->cell = headerCell->cell;
        newHeaderCell->next = nullptr;

        if (headerRow->cell == nullptr) {
            headerRow->cell = newHeaderCell;
        } else {
            lastHeaderCell->next = newHeaderCell;
        }
        lastHeaderCell = newHeaderCell;
        headerCell = headerCell->next;
    }
    

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    int condNum1 = -1;
    Node* currCell = table->cell;
    for (int i = 1; currCell != nullptr; i++) {
        if (currCell->name == name1) { 
            condNum1 = i;
            break;
        }
        currCell = currCell->next;
    }

    if (condNum1 == -1) {
        cout << "Колонка с именем " << name1 << " не найдена!" << endl;
        return nullptr;
    }

    int condNum2 = -1;
    Node* currCell2 = table->cell;
    for (int i = 1; currCell2 != nullptr; i++) {
        if (currCell2->name == name2) { 
            condNum2 = i;
            break;
        }
        currCell2 = currCell2->next;
    }

    if (condNum2 == -1) {
        cout << "Колонка с именем " << name2 << " не найдена!" << endl;
        return nullptr;
    }

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        Node* cell = currentRow->cell;
        int cellIndex = 1;
        bool rowMatches = false;

        while (cell != nullptr) {
            if ((cellIndex == condNum1 || cellIndex == condNum2) && (cell->cell == cond1 || cell->cell == cond2)) {
                rowMatches = true;
                break;
            }
            cell = cell->next;
            cellIndex++;
        }

        if (rowMatches) {
            RowNode* newRow = new RowNode;
            newRow->nextRow = nullptr;

            Node* newCell = nullptr;
            Node* newCellTail = nullptr;
            cell = currentRow->cell;

            while (cell != nullptr) {
                Node* copiedCell = new Node;
                copiedCell->name = cell->name;
                copiedCell->cell = cell->cell;
                copiedCell->next = nullptr;

                if (newCell == nullptr) {
                    newCell = copiedCell;
                    newCellTail = copiedCell;
                } else {
                    newCellTail->next = copiedCell;
                    newCellTail = newCellTail->next;
                }
                cell = cell->next;
            }
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;
    
    return headerRow;
}

RowNode* selectTwoTablesOneCond(RowNode* table, const string name1, const string name2) {
 
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;

    Node* lastHeaderCell = nullptr;
    Node* headerCell = table->cell;

    
    while (headerCell != nullptr) {
        Node* newHeaderCell = new Node;
        newHeaderCell->name = headerCell->name;
        newHeaderCell->cell = headerCell->cell;
        newHeaderCell->next = nullptr;

        if (headerRow->cell == nullptr) {
            headerRow->cell = newHeaderCell;
        } else {
            lastHeaderCell->next = newHeaderCell;
        }
        lastHeaderCell = newHeaderCell;
        headerCell = headerCell->next;
    }

    int condNum1 = -1;
    int condNum2 = -1;

    Node* currCell = table->cell;
    for (int i = 1; currCell != nullptr; i++) {
        if (condNum1 == -1){
            if (currCell->name == name1) { 
                condNum1 = i;
            }
        }
        if(i != condNum1){
            if (currCell->name == name2) { 
                condNum2 = i;
            }
        }
        currCell = currCell->next;
    }
    if (condNum1 == -1 || condNum2 == -1) {
        cout << "Колонка не найдена!" << endl;
        return nullptr;
    }

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        Node* cell = currentRow->cell;
        int cellIndex = 1;
        string value1, value2;
        bool rowMatches = false;

        while (cell != nullptr) {
            if (cellIndex == condNum1) {
                value1 = cell->cell;
            }
            if (cellIndex == condNum2) {
                value2 = cell->cell;
            }
            cell = cell->next;
            cellIndex++;
        }

        if (value1 == value2) {
            rowMatches = true;
        }

        if (rowMatches) {
            RowNode* newRow = new RowNode;
            newRow->nextRow = nullptr;

            Node* newCell = nullptr;
            Node* newCellTail = nullptr;
            cell = currentRow->cell;

            while (cell != nullptr) {
                Node* copiedCell = new Node;
                copiedCell->name = cell->name;
                copiedCell->cell = cell->cell;
                copiedCell->next = nullptr;

                if (newCell == nullptr) {
                    newCell = copiedCell;
                    newCellTail = copiedCell;
                } else {
                    newCellTail->next = copiedCell;
                    newCellTail = newCellTail->next;
                }
                cell = cell->next;
            }
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;
    return headerRow;
}

RowNode* selectTwoTablesTwoCond(RowNode* table, const string name1, const string name2, const string name, const string value) {
    
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;

    Node* lastHeaderCell = nullptr;
    Node* headerCell = table->cell;

    while (headerCell != nullptr) {
        Node* newHeaderCell = new Node;
        newHeaderCell->name = headerCell->name;
        newHeaderCell->cell = headerCell->cell;
        newHeaderCell->next = nullptr;

        if (headerRow->cell == nullptr) {
            headerRow->cell = newHeaderCell;
        } else {
            lastHeaderCell->next = newHeaderCell;
        }
        lastHeaderCell = newHeaderCell;
        headerCell = headerCell->next;
    }

    int condNum1 = -1;
    int condNum2 = -1;
    int condNum3 = -1;

    Node* currCell = table->cell;
    for (int i = 1; currCell != nullptr; i++) {
        if (condNum1 == -1){
            if (currCell->name == name1) { 
                condNum1 = i;
            }
        }
        if(i != condNum1){
            if (currCell->name == name2) { 
                condNum2 = i;
            }
        }
        if(i != condNum2){
            if (currCell->name == name) { 
                condNum3 = i;
            }
        }

        currCell = currCell->next;
    }
    if (condNum1 == -1 || condNum2 == -1 || condNum3 == -1) {
        cout << "Колонка не найдена!" << endl;
        return nullptr;
    }

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        Node* cell = currentRow->cell;
        int cellIndex = 1;
        string value1, value2, value3;
        bool rowMatches = false;

        while (cell != nullptr) {
            if (cellIndex == condNum1) {
                value1 = cell->cell;
            }
            if (cellIndex == condNum2) {
                value2 = cell->cell;
            }
            if (cellIndex == condNum3) {
                value3 = cell->cell;
            }
            cell = cell->next;
            cellIndex++;
        }

        if (value1 == value2 || value3 == value) {
            rowMatches = true;
        }

        if (rowMatches) {
            RowNode* newRow = new RowNode;
            newRow->nextRow = nullptr;

            Node* newCell = nullptr;
            Node* newCellTail = nullptr;
            cell = currentRow->cell;

            while (cell != nullptr) {
                Node* copiedCell = new Node;
                copiedCell->name = cell->name;
                copiedCell->cell = cell->cell;
                copiedCell->next = nullptr;

                if (newCell == nullptr) {
                    newCell = copiedCell;
                    newCellTail = copiedCell;
                } else {
                    newCellTail->next = copiedCell;
                    newCellTail = newCellTail->next;
                }
                cell = cell->next;
            }
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;
    return headerRow;
}

RowNode* cartesianProduct(RowNode** tables, int countTables) {
    
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;
    
    Node* lastHeaderCell = nullptr;
    
    for (int i = 0; i < countTables; ++i) {
        Node* headerCell = tables[i]->cell; 

        while (headerCell != nullptr) {
            Node* newHeaderCell = new Node;
            newHeaderCell->name = headerCell->name;
            newHeaderCell->cell = headerCell->cell;
            newHeaderCell->next = nullptr;

            if (headerRow->cell == nullptr) {
                headerRow->cell = newHeaderCell;
            } else {
                lastHeaderCell->next = newHeaderCell;
            }
            lastHeaderCell = newHeaderCell;
            headerCell = headerCell->next;
        }
    }

    RowNode* crossTable = nullptr;

    for (int i = 1; i < countTables; ++i) {
        RowNode* currentTable = tables[i];
        RowNode* newCrossTable = nullptr;
        RowNode* rowA = crossTable ? crossTable : tables[0]->nextRow;

        while (rowA != nullptr) {
            RowNode* rowB = currentTable->nextRow;

            while (rowB != nullptr) {
                RowNode* newRow = new RowNode;
                newRow->cell = nullptr;
                newRow->nextRow = nullptr;

                Node* currCellA = rowA->cell;
                Node* lastCell = nullptr;

                while (currCellA != nullptr) {
                    Node* newCell = new Node;
                    newCell->name = currCellA->name;
                    newCell->cell = currCellA->cell;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    } else {
                        lastCell->next = newCell;
                    }
                    lastCell = newCell;
                    currCellA = currCellA->next;
                }

                Node* currCellB = rowB->cell;
                while (currCellB != nullptr) {
                    Node* newCell = new Node;
                    newCell->name = currCellB->name;
                    newCell->cell = currCellB->cell;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    } else {
                        lastCell->next = newCell;
                    }
                    lastCell = newCell;
                    currCellB = currCellB->next;
                }

                if (newCrossTable == nullptr) {
                    newCrossTable = newRow;
                } else {
                    RowNode* lastRow = newCrossTable;
                    while (lastRow->nextRow != nullptr) {
                        lastRow = lastRow->nextRow;
                    }
                    lastRow->nextRow = newRow;
                }
                rowB = rowB->nextRow;
            }
            rowA = rowA->nextRow;
        }
        crossTable = newCrossTable;
    }

    headerRow->nextRow = crossTable;
    return headerRow;
}

RowNode* filteredTable(RowNode** tables, const string& name1, const string& cond1,const string& name2, const string& cond2, const string& oper){

    if(oper == "AND"){
        RowNode* cartesian = cartesianProduct(tables, 2);
        RowNode* result1 = selectFromTable(cartesian, name1, cond1);
        RowNode* result2 = selectFromTable(result1, name2, cond2);
        return result2;
    }

    if (oper == "OR"){
        RowNode* cartesian = cartesianProduct(tables, 2);
        RowNode* result = selectFromTables(cartesian, name1, cond1, name2, cond2);
        return result;
    }

    return 0;
}

RowNode* deleteFrom(RowNode* table, const string& name, const string& cond) {
    
    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;

    Node* lastHeaderCell = nullptr;
    Node* headerCell = table->cell;

    while (headerCell != nullptr) {
        Node* newHeaderCell = new Node;
        newHeaderCell->name = headerCell->name;
        newHeaderCell->cell = headerCell->cell;
        newHeaderCell->next = nullptr;

        if (headerRow->cell == nullptr) {
            headerRow->cell = newHeaderCell;
        } else {
            lastHeaderCell->next = newHeaderCell;
        }
        lastHeaderCell = newHeaderCell;
        headerCell = headerCell->next;
    }

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    int condNum = -1;
    Node* currCell = table->cell;
    for (int i = 1; currCell != nullptr; i++) {
        if (currCell->name == name) { 
            condNum = i;
            break;
        }
        currCell = currCell->next;
    }

    if (condNum == -1) {
        cout << "Колонка с именем " << name << " не найдена!" << endl;
        return nullptr;
    }

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        Node* cell = currentRow->cell;
        int cellIndex = 1;
        bool rowMatches = true;

        while (cell != nullptr) {
            if (cellIndex == condNum && cell->cell == cond) {
                rowMatches = false;
                break;
            }
            cell = cell->next;
            cellIndex++;
        }

        if (rowMatches) {
            RowNode* newRow = new RowNode;
            newRow->nextRow = nullptr;

            Node* newCell = nullptr;
            Node* newCellTail = nullptr;
            cell = currentRow->cell;

            while (cell != nullptr) {
                Node* copiedCell = new Node;
                copiedCell->name = cell->name;
                copiedCell->cell = cell->cell;
                copiedCell->next = nullptr;

                if (newCell == nullptr) {
                    newCell = copiedCell;
                    newCellTail = copiedCell;
                } else {
                    newCellTail->next = copiedCell;
                    newCellTail = newCellTail->next;
                }
                cell = cell->next;
            }
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;
    
    return headerRow;
}

RowNode* deleteFromWithOper(RowNode* table, const string& name1, const string& cond1, const string& name2, const string& cond2, const string& oper) {

    RowNode* headerRow = new RowNode;
    headerRow->cell = nullptr;
    headerRow->nextRow = nullptr;
    
    Node* lastHeaderCell = nullptr;
    
   
    Node* headerCell = table->cell;

    while (headerCell != nullptr) {
        Node* newHeaderCell = new Node;
        newHeaderCell->name = headerCell->name;
        newHeaderCell->cell = headerCell->cell;
        newHeaderCell->next = nullptr;

        if (headerRow->cell == nullptr) {
            headerRow->cell = newHeaderCell;
        } else {
            lastHeaderCell->next = newHeaderCell;
        }
        lastHeaderCell = newHeaderCell;
        headerCell = headerCell->next;
    }

    RowNode* newTable = nullptr;
    RowNode* newTableTail = nullptr;

    int condNum1 = -1;
    Node* currCell = table->cell;
    for (int i = 1; currCell != nullptr; i++) {
        if (currCell->name == name1) { 
            condNum1 = i;
            break;
        }
        currCell = currCell->next;
    }

    if (condNum1 == -1) {
        cout << "Колонка с именем " << name1 << " не найдена!" << endl;
        return nullptr;
    }

    int condNum2 = -1;
    Node* currCell2 = table->cell;
    for (int i = 1; currCell2 != nullptr; i++) {
        if (currCell2->name == name2) { 
            condNum2 = i;
            break;
        }
        currCell2 = currCell2->next;
    }

    if (condNum2 == -1) {
        cout << "Колонка с именем " << name2 << " не найдена!" << endl;
        return nullptr;
    }

    RowNode* currentRow = table->nextRow;
    while (currentRow != nullptr) {
        Node* cell = currentRow->cell;
        int cellIndex = 1;
        bool condition1Met = false;
        bool condition2Met = false;

        while (cell != nullptr) {
            if (cellIndex == condNum1 && cell->cell == cond1) {
                condition1Met = true;
            }
            if (cellIndex == condNum2 && cell->cell == cond2) {
                condition2Met = true;
            }
            cell = cell->next;
            cellIndex++;
        }

        bool rowMatches = false;
        if (oper == "AND") {
            rowMatches = condition1Met && condition2Met;
        } else if (oper == "OR") {
            rowMatches = condition1Met || condition2Met;
        }

        if (!rowMatches) {
            RowNode* newRow = new RowNode;
            newRow->nextRow = nullptr;

            Node* newCell = nullptr;
            Node* newCellTail = nullptr;
            cell = currentRow->cell;

            while (cell != nullptr) {
                Node* copiedCell = new Node;
                copiedCell->name = cell->name;
                copiedCell->cell = cell->cell;
                copiedCell->next = nullptr;

                if (newCell == nullptr) {
                    newCell = copiedCell;
                    newCellTail = copiedCell;
                } else {
                    newCellTail->next = copiedCell;
                    newCellTail = newCellTail->next;
                }
                cell = cell->next;
            }
            newRow->cell = newCell;

            if (newTable == nullptr) {
                newTable = newRow;
                newTableTail = newRow;
            } else {
                newTableTail->nextRow = newRow;
                newTableTail = newTableTail->nextRow;
            }
        }

        currentRow = currentRow->nextRow;
    }

    headerRow->nextRow = newTable;
    
    return headerRow;
}

void lockTables(const string& tableName, bool parameter) {

    string schema = schemaName("schema.json");
    string directory = schema + "/" + tableName;
    string fileName = directory + "/" + tableName + "_lock.txt";
    
    filesystem::create_directories(directory);
    
    ofstream lockFile(fileName, ios::binary);
    if (lockFile.is_open()) {
        lockFile << 0;
        lockFile.close();
    } else {
        cerr << "Ошибка создания файла блокировки!" << endl;
        return;
    }

    ifstream inputFile(fileName);
    if (!inputFile) {
        cerr << "Ошибка открытия файла для чтения!" << endl;
        return;
    }

    string currValueStr;
    inputFile >> currValueStr;
    inputFile.close();

    int currValue = stoi(currValueStr);
    if (parameter) {
        currValue++;
    } else {
        currValue--;
    }

    ofstream outputFile(fileName);
    if (!outputFile) {
        cerr << "Ошибка открытия файла для записи!" << endl;
        return;
    }

    outputFile << currValue;
    outputFile.close();
}

void splitConditions(const string& input, string& condition1, string& condition2, string& oper) {
    
    size_t orPos = input.find(" OR ");
    size_t andPos = input.find(" AND ");

    size_t splitPos;
    if (orPos != string::npos && (andPos == string::npos || orPos < andPos)) {
        splitPos = orPos;
        oper = "OR";
    } else if (andPos != string::npos) {
        splitPos = andPos;
        oper = "AND";
    } else {
        condition1 = input;
        condition2 = "";
        oper = "None";
        return;
    }

    condition1 = input.substr(0, splitPos);
    condition2 = input.substr(splitPos + (oper == "OR" ? 4 : 5));
}

void extractNamesTable(const string& input, string& tableName, string& columnName, string& value) {

    size_t dotPos = input.find('.');
    size_t equalPos = input.find('=');
    size_t quoteStart = input.find('\'', equalPos);
    size_t quoteEnd = input.find('\'', quoteStart + 1);

    if (dotPos == string::npos || equalPos == string::npos || quoteStart == string::npos || quoteEnd == string::npos) {
        cerr << "Ошибка формата строки!" << endl;
        return;
    }

    tableName = input.substr(0, dotPos);

    columnName = input.substr(dotPos + 1, equalPos - dotPos - 2);
    value = input.substr(quoteStart + 1, quoteEnd - quoteStart - 1);
}

void extractNamesTables(const string& input, string& table1, string& column1, string& table2, string& column2) {
    
    size_t dotPos1 = input.find('.');      
    size_t equalsPos = input.find(" = ");  
    size_t dotPos2 = input.find('.', equalsPos);

    if (dotPos1 != string::npos) {
        table1 = input.substr(0, dotPos1);
        column1 = input.substr(dotPos1 + 1, equalsPos - dotPos1 - 1);
    }

    if (dotPos2 != string::npos) {
        table2 = input.substr(equalsPos + 3, dotPos2 - equalsPos - 3);
        column2 = input.substr(dotPos2 + 1);
    }
}

int main() {
	
    setlocale(LC_ALL, "ru"); 
    cout << "        ~Список команд~" << endl;
    cout << "SELECT             ---   select" << endl;
    cout << "INSERT INTO        ---   insin" << endl;
    cout << "DELETE FROM        ---   del" << endl;
    cout << "CREATE DIRECTORY   ---   cdir" << endl;
    cout << "PRINT TABLE        ---   ptab" << endl;
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
            cout << "SELECT             ---   select" << endl;
            cout << "INSERT INTO        ---   insin" << endl;
            cout << "DELETE FROM        ---   del" << endl;
            cout << "CREATE DIRECTORY   ---   cdir" << endl;
            cout << "PRINT TABLE        ---   ptab" << endl;
            cout << "HELP               ---   help" << endl;
            cout << "EXIT               ---   exit" << endl << endl;
        }
        if(command == "select"){
            string schema = schemaName("schema.json");
            cout << "Условие: ";
            string input;
            getline(cin, input);
            cout << endl;
            
            string cond1, cond2, oper;
            splitConditions(input, cond1, cond2, oper);
            if (oper == "None"){
                if(cond1.find("'") != string::npos){
                    string tableName, columnName, value;
                    extractNamesTable(cond1, tableName, columnName, value);
                    lockTables(tableName, 1);
                    RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
                    RowNode* result = selectFromTable(table, columnName, value);
                    printTableSecond(result);
                    lockTables(tableName, 0);
                    freeOneTable(table);
                    freeOneTable(result);
                }
                else{
                    string tableName1, column1, tableName2, column2;
                    extractNamesTables(input, tableName1, column1, tableName2, column2);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* cartesian = cartesianProduct(tables, 2);
                    RowNode* result = selectTwoTablesOneCond(cartesian, column1, column2);
                    printTableSecond(result);
                    lockTables(tableName1, 0);
                    lockTables(tableName2, 0);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(cartesian);
                    freeOneTable(result);
                }
            }
            if (oper == "AND"){
                if((cond1.find("'") != string::npos) && (cond2.find("'") != string::npos)){
                    string tableName1, columnName1, value1;
                    extractNamesTable(cond1, tableName1, columnName1, value1);
                    string tableName2, columnName2, value2;
                    extractNamesTable(cond2, tableName2, columnName2, value2);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* result = filteredTable(tables, columnName1, value1, columnName2, value2, oper);
                    printTableSecond(result);
                    lockTables(tableName1, 0);
                    lockTables(tableName2, 0);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(result);
                }
                if ((cond1.find("'") == string::npos) && (cond2.find("'") != string::npos)) {
                    string tableName1, column1, tableName2, column2;
                    extractNamesTables(cond1, tableName1, column1, tableName2, column2);
                    string tableName, columnName, value;
                    extractNamesTable(cond2, tableName, columnName, value);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* cartesian = cartesianProduct(tables, 2);
                    RowNode* temp = selectTwoTablesOneCond(cartesian, column1, column2);
                    RowNode* result = selectFromTable(temp, columnName, value);
                    printTableSecond(result);
                    cout << endl;
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(table);
                    freeOneTable(cartesian);
                    freeOneTable(temp);
                    freeOneTable(result);
                }
                if ((cond1.find("'") != string::npos) && (cond2.find("'") == string::npos)) {
                    string tableName1, column1, tableName2, column2;
                    extractNamesTables(cond2, tableName1, column1, tableName2, column2);
                    string tableName, columnName, value;
                    extractNamesTable(cond1, tableName, columnName, value);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* cartesian = cartesianProduct(tables, 2);
                    RowNode* temp = selectTwoTablesOneCond(cartesian, column1, column2);
                    RowNode* result = selectFromTable(temp, columnName, value);
                    printTableSecond(result);
                    cout << endl;
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(table);
                    freeOneTable(cartesian);
                    freeOneTable(temp);
                    freeOneTable(result);
                }
            }
            if (oper == "OR"){
                if((cond1.find("'") != string::npos) && (cond2.find("'") != string::npos)){
                    string tableName1, columnName1, value1;
                    extractNamesTable(cond1, tableName1, columnName1, value1);
                    string tableName2, columnName2, value2;
                    extractNamesTable(cond2, tableName2, columnName2, value2);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* result = filteredTable(tables, columnName1, value1, columnName2, value2, oper);
                    printTableSecond(result);
                    lockTables(tableName1, 0);
                    lockTables(tableName2, 0);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(result);
                }
                if ((cond1.find("'") == string::npos) && (cond2.find("'") != string::npos)) {
                    string tableName1, column1, tableName2, column2;
                    extractNamesTables(cond1, tableName1, column1, tableName2, column2);
                    string tableName, columnName, value;
                    extractNamesTable(cond2, tableName, columnName, value);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* cartesian = cartesianProduct(tables, 2);
                    RowNode* result = selectTwoTablesTwoCond(cartesian, column1, column2, columnName, value);
                    printTableSecond(result);
                    cout << endl;
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(table);
                    freeOneTable(cartesian);
                    freeOneTable(result);
                }
                if ((cond1.find("'") != string::npos) && (cond2.find("'") == string::npos)) {
                    string tableName1, column1, tableName2, column2;
                    extractNamesTables(cond2, tableName1, column1, tableName2, column2);
                    string tableName, columnName, value;
                    extractNamesTable(cond1, tableName, columnName, value);
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    RowNode* table1 = convertCSVToLinkedList(schema + "/" + tableName1);
                    RowNode* table2 = convertCSVToLinkedList(schema + "/" + tableName2);
                    RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
                    RowNode** tables = new RowNode * [2];
                    tables[0] = table1;
                    tables[1] = table2;
                    RowNode* cartesian = cartesianProduct(tables, 2);
                    RowNode* result = selectTwoTablesTwoCond(cartesian, column1, column2, columnName, value);
                    printTableSecond(result);
                    cout << endl;
                    lockTables(tableName1, 1);
                    lockTables(tableName2, 1);
                    lockTables(tableName, 1);
                    freeOneTable(table1);
                    freeOneTable(table2);
                    freeOneTable(table);
                    freeOneTable(cartesian);
                    freeOneTable(result);
                }
            }
        }
        if (command == "cdir"){
            cout << endl;
            createSchemaStructure("schema.json");
            cout << endl;
        }
        if (command == "ptab"){
            string schema = schemaName("schema.json");
            string tableName;
            cout << "> Выберите таблицу: ";
            cin >> tableName;
            lockTables(tableName, 1);
            cout << endl;
            RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
            cout << tableName << ":" << endl;
            printTableSecond(table);
            lockTables(tableName, 0);
            freeOneTable(table);
            cout << endl;
        }
        if (command == "insin"){
            cout << "Выберите таблицу: ";
            string tableName;
            cin >> tableName;
            cout << "Вводите данные строки:" << endl;
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            string listString[100];
            int count = 0;
            while (count < 100) { 
                string input;
                getline(cin, input);
                if (input.empty()) {
                    break;
                }
                listString[count++] = input;
            }
            int tuples = tuplesLimit("schema.json");
            RowNode* temp = nullptr;
            lockTables(tableName, 1);
            temp = insertInto(temp, listString, tableName, tuples, "schema.json");
            lockTables(tableName, 0);
            freeOneTable(temp);
        }
        if (command == "del"){
            string schema = schemaName("schema.json");
            cout << "Условие: ";
            string input;
            getline(cin, input);
            cout << endl;

            string cond1, cond2, oper;
            splitConditions(input, cond1, cond2, oper);
            if (oper == "None") {
                string tableName, columnName, value;
                extractNamesTable(cond1, tableName, columnName, value);
                lockTables(tableName, 1);
                RowNode* table = convertCSVToLinkedList(schema + "/" + tableName);
                RowNode* result = deleteFrom(table, columnName, value);
                convertToCSV(result, schema + "/" + tableName);
                lockTables(tableName, 0);
                freeOneTable(table);
                freeOneTable(result);
            }
            if (oper == "AND" || oper == "OR") {
                string tableName1, columnName1, value1;
                extractNamesTable(cond1, tableName1, columnName1, value1);
                string tableName2, columnName2, value2;
                extractNamesTable(cond2, tableName2, columnName2, value2);
                lockTables(tableName1, 1);
                RowNode* table = convertCSVToLinkedList(schema + "/" + tableName1);
                RowNode* result = deleteFromWithOper(table, columnName1, value1, columnName2, value2, oper);
                convertToCSV(result, schema + "/" + tableName1);
                lockTables(tableName1, 0);
                freeOneTable(table);
                freeOneTable(result);
            }
        }
    }
    
	return 0;
}
