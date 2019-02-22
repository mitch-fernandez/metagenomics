#Extracts Richness, Diversity, and other measures from Summary.single at the Kingdom level
import os
path = "c:\Sync Folder\Biorg\Python\Data"
listing = os.listdir(path)
file_list = []
for infile in listing:
    file_list.append(infile.replace('.Summary.single.txt', ''))

#Create a database
import sqlite3
db = sqlite3.connect("c:\Sync Folder\Biorg\Python\Diversity.db")
cursor = db.cursor()
cursor.execute('''DROP TABLE Diversity''')
cursor.execute('''CREATE TABLE Diversity(Calculator UNIQUE)''')
cursor.execute("INSERT INTO Diversity VALUES ('NSEQS')")
cursor.execute("INSERT INTO Diversity VALUES ('SOBS')")
cursor.execute("INSERT INTO Diversity VALUES ('Coverage')")
cursor.execute("INSERT INTO Diversity VALUES ('Chao')")
cursor.execute("INSERT INTO Diversity VALUES ('Inverse_Simpson')")
cursor.execute("INSERT INTO Diversity VALUES ('NP_Shannon')")

#Add columns from filenames
for i in file_list:
    cursor.execute('''ALTER TABLE Diversity ADD COLUMN %s INTEGER DEFAULT (0)''' % i)

#Populate database
import glob
for infile in glob.glob(os.path.join(path, '*.txt')):
    with open(infile) as Calcs:
        patient = str(infile).replace('.Summary.single.txt', '').replace(path, '').replace('\\', '')
        for line in Calcs:
            Calcs_list = line.split('\t')
            if Calcs_list[0] == 'label':
                print('Skipping line')
            elif Calcs_list[0] == '6':
                Calcs_nseqs = Calcs_list[4]
                Calcs_sobs = Calcs_list[10]
                Calcs_coverage = Calcs_list[5]
                Calcs_chao = Calcs_list[11]
                Calcs_invsimpson = Calcs_list[1]
                Calcs_npshannon = Calcs_list[6]
                sql = "UPDATE Diversity SET  " + patient + " = ? WHERE Calculator = ?"
                cursor.execute(sql, (Calcs_nseqs, 'NSEQS'))
                cursor.execute(sql, (Calcs_sobs, 'SOBS'))
                cursor.execute(sql, (Calcs_coverage, 'Coverage'))
                cursor.execute(sql, (Calcs_chao, 'Chao'))
                cursor.execute(sql, (Calcs_invsimpson, 'Inverse_Simpson'))
                cursor.execute(sql, (Calcs_npshannon, 'NP_Shannon'))
                
import csv
cursor.execute('SELECT * FROM Diversity')
csv_writer = csv.writer(open("c:\Sync Folder\Biorg\Python\Richness_And_Diversity_Table_Kingdom.csv", "wt"))
csv_writer.writerow([i[0] for i in cursor.description]) # write headers
csv_writer.writerows(cursor)
del csv_writer # this will close the CSV file
