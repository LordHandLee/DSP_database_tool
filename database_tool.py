import sys
import os
from PyQt5.QtGui import QColor, QFont
import sqlite3


from PyQt5.QtWidgets import QApplication, QComboBox, QFileDialog, QLabel, QListView, QListWidget, QTextBrowser, QWidget, QPushButton, QStackedWidget, QTextEdit
from PyQt5.QtCore import QFile, Qt
#from PyQt5.QtUiTools import QUiLoader
from PyQt5 import uic

import pandas as pd
from pandas.io import sql


class MainWindow(QWidget):
    def __init__(self):
        super(MainWindow, self).__init__()
        uic.loadUi("DatabaseHiveTool.ui", self)
        for i in self.findChildren(QPushButton):
            print(i.objectName())
            if i.objectName() == "loadCSV":
                i.clicked.connect(self.load_csv)
            if i.objectName() == "loadSQL":
                i.clicked.connect(self.load_sql)
            if i.objectName() == "selectDir":
                i.clicked.connect(self.select_direct)
            if i.objectName() == "data_cleanup":
                i.clicked.connect(self.dataframe_cleanup)
            if i.objectName() == "df_to_db":
                i.clicked.connect(self.dataframe_db)
            if i.objectName() == "df_to_csv":
                i.clicked.connect(self.dataframe_csv)
            if i.objectName() == "merge":
                i.clicked.connect(self.merge1)
            if i.objectName() == "concat":
                i.clicked.connect(self.concat1)
            if i.objectName() == "stats":
                i.clicked.connect(self.stats1)
        for i in self.findChildren(QTextEdit):
            print(i.objectName())
            if i.objectName() == "textEdit":
                self.textEdit1 = i
                self.textEdit1.setReadOnly(True)
                self.textEdit1.setLineWrapMode(QTextEdit.NoWrap)
                self.textEdit1.setCurrentFont(QFont("Courier New"))
            if i.objectName() == "textEdit_2":
                self.textEdit2 = i
                self.textEdit2.setReadOnly(True)
                self.textEdit2.setLineWrapMode(QTextEdit.NoWrap)
                self.textEdit2.setCurrentFont(QFont("Courier New"))
        #pd.set_option('max_rows', None)
        #pd.set_option('max_columns', None)


    def load_csv(self, other):
        """Reads csv into dataframe and displays into textEdit widget.
        If textEdit is not empty, store into textEdit2. If both are not empty,
        clear both widgets and populate the first one (reset)."""
        # if other:
        #     #sql1 = QfileDialog.getOpenFileName(self)
        #     self.csv_dataframe = pd.read_sql(other, self.conn) #read sql into dataframe
        # else:
        #     csv1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file csv",
        #                                                          filter="SQL Files (*.csv)") # open csv file
        #     #self.csv1 = []
        #     path_i = csv1[0].rindex("/")
        #     self.csv1 = csv1[0][path_i+1:].strip(".csv")
        #     print(self.csv1, "csv1")
        #     self.csv_dataframe = pd.read_csv(csv1[0]) # read csv into dataframe
        if len(self.textEdit1.toPlainText()) != 0 and len(self.textEdit2.toPlainText()) != 0:
            self.textEdit1.clear()
            self.textEdit2.clear()
            print("populate 1")
            if other:
                #sql1 = QfileDialog.getOpenFileName(self)
                self.csv_dataframe = pd.read_sql(other, self.conn) #read sql into dataframe
            else:
                csv1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file csv",
                                                                        filter="SQL Files (*.csv)") # open csv file
                #self.csv1 = []
                path_i = csv1[0].rindex("/")
                self.csv1 = csv1[0][path_i+1:].strip(".csv")
                print(self.csv1, "csv1")
                self.csv_dataframe = pd.read_csv(csv1[0]) # read csv into dataframe
            self.textEdit1.insertPlainText(self.csv_dataframe.to_string(index=False))
            #print(self.csv_dataframe.to_string)
            self.textEdit1.verticalScrollBar()
            self.textEdit1.horizontalScrollBar()
            return
        if len(self.textEdit1.toPlainText()) != 0:
            if other:
                #sql1 = QfileDialog.getOpenFileName(self)
                self.csv_dataframe2 = pd.read_sql(other, self.conn) #read sql into dataframe
            else:
                csv1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file csv",
                                                                        filter="SQL Files (*.csv)") # open csv file
                #self.csv1 = []
                path_i = csv1[0].rindex("/")
                self.csv2 = csv1[0][path_i+1:].strip(".csv")
                print(self.csv2, "csv2")
                self.csv_dataframe2 = pd.read_csv(csv1[0]) # read csv into dataframe
            print("populate 2")
            self.textEdit2.insertPlainText(self.csv_dataframe2.to_string(index=False))
            self.textEdit2.verticalScrollBar()
            self.textEdit2.horizontalScrollBar()
            return
        if len(self.textEdit1.toPlainText()) == 0 and len(self.textEdit2.toPlainText()) == 0: #insert into first
            if other:
                #sql1 = QfileDialog.getOpenFileName(self)
                self.csv_dataframe = pd.read_sql(other, self.conn) #read sql into dataframe
            else:
                csv1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file csv",
                                                                        filter="SQL Files (*.csv)") # open csv file
                #self.csv1 = []
                path_i = csv1[0].rindex("/")
                self.csv1 = csv1[0][path_i+1:].strip(".csv")
                print(self.csv1, "csv1")
                self.csv_dataframe = pd.read_csv(csv1[0]) # read csv into dataframe
            print("populate 1")
            self.textEdit1.insertPlainText(self.csv_dataframe.to_string(index=False))
            self.textEdit1.verticalScrollBar()
            self.textEdit1.horizontalScrollBar()
            #print(self.csv_dataframe.to_string())
            return
        pass
    def load_sql(self): #TODO# update file dialog to only accept SQL files or CSV files
        self.sql1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file",
                                                                 filter="SQL Files (*.db)")
        self.conn = sqlite3.connect(self.sql1[0])
        self.curr = self.conn.cursor()
        path_i = self.sql1[0].rindex("/")
        self.table1 = self.sql1[0][path_i+1:].strip(".db")
        dapath = (
            "SELECT * FROM {}").format(self.table1)
        print(self.conn, self.table1)
        self.load_csv(dapath)
        pass
    def select_direct(self):
        self.dir1 = QFileDialog.getExistingDirectory(self)
        print(self.dir1)
        pass
    def dataframe_cleanup(self):
        """Compares the filename in the target directory to the filename
        in the current dataframe. Removes any entry in the dataframe not in
        the image directory."""
        self.fileList = []
        self.indexList = []
        #print(self.dir1, self.csv_dataframe)
        #print(self.dir1 is not None)

        if self.dir1 is not None and self.csv_dataframe is not None:
            for i in os.listdir(self.dir1):
                #hash file names here
                self.fileList.append(i)
            for index, row in self.csv_dataframe.iterrows():
                if row['original_filename'] in self.fileList:
                    print(row["original_filename"])
                    self.indexList.append(index)
            print(self.indexList)
            #for i in self.indexList:
            self.csv_dataframe.drop(self.indexList, inplace=True)
            print(self.csv_dataframe.head(8))
            self.textEdit.clear()
            self.textEdit.insertPlainText(self.csv_dataframe.to_string(index=False))
            # for index, row in self.csv_dataframe.iterrows():
            #     if row['original_filename'] in self.fileList:
            #         print(row["original_filename"])
            #print(self.csv_dataframe.to_string())
                    # remove row by index

                    # update corresponding QTextEdit widget with new dataframe

                #access hash column instead of original_filename column for verification


        pass
    def dataframe_db(self):
        """Converts dataframe to a SQL database"""
        if self.csv1 is not None: # CSV file has been loaded into dataframe
            column_list = []
            self.table2 = self.csv1 + ".db"
            print(self.csv1, self.table2)
            self.new_conn = sqlite3.connect(self.table2)
            c = self.new_conn.cursor()
            for column in self.csv_dataframe.columns:
                print(column)
                column_list.append(column)
            # column_final = "".join("{}".format("{} \n") for i in range(len(column_list)))
            # c2 = "".join("{}".format(i + "\n") for i in column_list)
            # print("c2", c2)
            # print("Column final: ", column_final)
            # c.execute('''CREATE TABLE {} (
            #     {}
            # )'''.format(self.csv1, c2))
            # parse thru csv files to get columns to create SQL
            print(self.new_conn)
            #sql1 = self.csv_dataframe.to_sql(self.csv1, self.new_conn)
            self.csv_dataframe.to_sql(self.csv1, self.new_conn, index=False)
            # with open(self.csv1 + ".db", 'w') as file:
            #     file.write(self.csv_dataframe.to_string())
            
        pass
    def dataframe_csv(self):
        """Converts dataframe to a CSV file"""
        try:
            if self.sql1 is not None:
                self.csv_dataframe.to_csv(self.table1 + ".csv", index=False)
        except Exception as e:
            if self.csv1 is not None:
                self.csv_dataframe.to_csv(self.csv1 + ".csv", index=False)
        pass

    def merge1(self):
        print("Hello")
        if len(self.textEdit1.toPlainText()) != 0 and len(self.textEdit2.toPlainText()) != 0:
            print("Dataframe : ", self.csv_dataframe, "Dataframe 2: ", self.csv_dataframe2)
            merged = pd.merge(self.csv_dataframe, self.csv_dataframe2, how="inner", on='original_filename')
            try:
                if self.csv1 is not None:
                    merged.to_csv(self.csv1 + "_MERGED.csv", index=False, encoding='utf-8')
            except Exception as e:
                if self.sql1 is not None:
                    merged.to_csv(self.sql1 + "_MERGED.csv", index=False, encoding='utf-8')
            pass
        pass
    
    def concat1(self):
        print('concat1')
        if len(self.textEdit1.toPlainText()) != 0 and len(self.textEdit2.toPlainText()) != 0:
            concat_list = [self.csv_dataframe, self.csv_dataframe2]
            concatenated = pd.concat(concat_list)
            try:
                if self.csv1 is not None:
                    concatenated.to_csv(self.csv1 + "_CONCATENATED.csv", index=False, encoding='utf-8')
            except Exception as e:
                if self.sql1 is not None:
                    concatenated.to_csv(self.sql1 + "_CONCATENATED.csv", index=False, encoding='utf-8')
            pass
        pass
    def stats1(self):
        Stacker.setCurrentIndex(1)

class StatsWindow(QWidget):
    def __init__(self, other):
        super(StatsWindow, self).__init__()
        uic.loadUi("StatsMenu.ui", self)
        self.other = other

        for i in self.findChildren(QPushButton):
            print(i.objectName())
            if i.objectName() == "go_back":
                i.clicked.connect(self.go_back1)
            if i.objectName() == "select_country":
                i.clicked.connect(self.select_country1)
            if i.objectName() == "select_dataset":
                i.clicked.connect(self.select_dataset1)
            if i.objectName() == "select_country_csv":
                i.clicked.connect(self.select_country_csv1)
            if i.objectName() == "select_dataset_csv":
                i.clicked.connect(self.select_dataset_csv1)
            pass
        for i in self.findChildren(QLabel):
            print(i.objectName())
            if i.objectName() == "country_path":
                self.country_path = i
            if i.objectName() == "dataset_path":
                self.dataset_path = i
            if i.objectName() == "country_label":
                self.country_label = i
            if i.objectName() == "male_count":
                self.male_count = i
            if i.objectName() == "female_count":
                self.female_count = i
            pass
        for i in self.findChildren(QTextEdit):
            print(i.objectName())
            if i.objectName() == "textEdit":
                self.textEdit = i
                self.textEdit.setReadOnly(True)
                self.textEdit.setLineWrapMode(QTextEdit.NoWrap)
                self.textEdit.setCurrentFont(QFont("Courier New"))
                
            pass
    def go_back1(self):
        Stacker.setCurrentIndex(0)
    def select_country1(self):
        self.male_list = []
        self.female_list = []
        self.cpath = QFileDialog.getExistingDirectory(self)
        self.country_path.setText(self.cpath)
        country_i = self.cpath.rindex("/")
        country = self.cpath[country_i+1:]
        self.country_label.setText(country)
        for file in os.listdir(self.cpath):
            if file.endswith(".jpg") or file.endswith(".png"):
                try:
                    file = file.index("_M_")
                    self.male_list.append(file)
                except:
                    file = file.index("_F_")
                    self.female_list.append(file)
                #print(file)
        self.male_count.setText(str(len(self.male_list)))
        self.female_count.setText(str(len(self.female_list)))
        pass
    def select_dataset1(self):
        self.dataset_dict = {}
        self.dpath = QFileDialog.getExistingDirectory(self)
        self.dataset_path.setText(self.dpath)
        for file in os.listdir(self.dpath):
            dir = os.path.join(self.dpath, file)
            if os.path.isdir(dir):
                print(dir)
                country_i = dir.rindex("/")
                country = dir[country_i+1:]
                print(country)
                male_list = []
                female_list = []
                malefemale = {"male":male_list, "female":female_list}
                for i in os.listdir(dir): #TODO# may need to change the way we traverse the files in case of unnacounted subdirectories
                    if i.endswith(".jpg") or i.endswith(".png"):
                        try:
                            i.index("_M_")
                            male_list.append(i)
                        except:
                            i.index("_F_")
                            female_list.append(i)
                self.dataset_dict[country] = malefemale
        print(self.dataset_dict)

        the_string = "".join(["Country:\t{}\tMale:\t{}\tFemale:\t{}      \n".format(key,len(value['male']), len(value['female'])) for key, value in self.dataset_dict.items()])
        print("THE STRING: \n", the_string)
        self.textEdit.insertPlainText(the_string)
        self.textEdit.verticalScrollBar()
        self.textEdit.horizontalScrollBar()


        pass
    def select_country_csv1(self):
        male_list = []
        female_list = []
        csv1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file csv",
                                                                        filter="SQL Files (*.csv)") # open csv file
        self.country_path.setText(csv1[0])
        csv_data = pd.read_csv(csv1[0])
        #TODO# update country label
        country_i = csv1[0].rindex("/")
        country = csv1[0][country_i+1:].strip(".csv")
        self.country_label.setText(country)
        print("COUNTRYL: ", country)
        for index, row in csv_data.iterrows():
            string = str(row['original_filename'])
            try:
                string_i = string.index("_M_")
                male_list.append(string_i)
            except:
                string_i = string.index("_F_")
                female_list.append(string_i)
        self.male_count.setText(str(len(male_list)))
        self.female_count.setText(str(len(female_list)))

        pass
    def select_dataset_csv1(self):
        print("hello")
        #male_list = []
        #female_list = []
        dataset_dict = {}
        csv1 = QFileDialog.getOpenFileName(self, caption="Open Meta data file csv",
                                                                        filter="SQL Files (*.csv)") # open csv file
        self.dataset_path.setText(csv1[0])
        csv_data = pd.read_csv(csv1[0])
        for index, row in csv_data.iterrows():
            string = str(row['original_filename'])
            male_list = []
            female_list = []
            malefemale = {"male":male_list, "female":female_list}
            try:
                string_i = string.index("_M_")
                country = string[:string_i]
                string_j = string.index("_")
                country = country[string_j+1:]
                if country in dataset_dict.keys():
                    dataset_dict[country]["male"].append(string_i)
                else:
                    dataset_dict[country] = malefemale
                    dataset_dict[country]["male"] = male_list
                    male_list.append(string_i)
            except:
                string_i = string.index("_F_")
                country = string[:string_i]
                string_j = string.index("_")
                country = country[string_j+1:]
                if country in dataset_dict.keys():
                    dataset_dict[country]["female"].append(string_i)
                else:
                    dataset_dict[country] = malefemale
                    dataset_dict[country]["female"] = female_list
                    female_list.append(string_i)
                
            #country_i = string.index
        #print(dataset_dict)
        the_string = "".join(["Country:\t{}\tMale:\t{}\tFemale:\t{}      \n".format(key,len(value['male']), len(value['female'])) for key, value in dataset_dict.items()])
        print("THE STRING: \n", the_string)
        self.textEdit.insertPlainText(the_string)
        self.textEdit.verticalScrollBar()
        self.textEdit.horizontalScrollBar()

        pass

if __name__ == "__main__":
    app = QApplication([])
    Stacker = QStackedWidget()
    mainW = MainWindow()
    statsW = StatsWindow(mainW)

    Stacker.setFixedHeight(1000)
    Stacker.setFixedWidth(1500)
    Stacker.addWidget(mainW) #index 0
    Stacker.addWidget(statsW)
    Stacker.show()

    #mainW.show()
    sys.exit(app.exec_())