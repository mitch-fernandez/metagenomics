import os
path = "c:\Sync Folder\Biorg\Python\Data"
listing = os.listdir(path)
file_list = []
for infile in listing:
	file_list.append(infile.replace('.Classify.otu.level6.taxonomy.txt', ''))

#Create a database
import sqlite3
db = sqlite3.connect("c:\Sync Folder\Biorg\Python\Reads.db")
cursor = db.cursor()
cursor.execute('''DROP TABLE Reads''')
cursor.execute('''CREATE TABLE Reads(Taxon Unique, Kingdom)''')

#Add columns from filenames
for i in file_list:
	cursor.execute('''ALTER TABLE Reads ADD COLUMN %s INTEGER DEFAULT (0)''' % i)

#Populate database
import glob
for infile in glob.glob(os.path.join(path, '*.txt')):
	with open(infile) as OTUs:
		patient = str(infile).replace('.Classify.otu.level6.taxonomy.txt', '').replace(path, '').replace('\\', '')
		for line in OTUs:
			OTU_count, OTU_size, OTU_tax = line.split('\t')
			if OTU_size == 'Size':
				print('Skipping first line')
			else:
				taxa = OTU_tax.split(';')
				OTU_taxon = taxa[0]
				OTU_tax = taxa[0]
				OTU_tax = OTU_tax.replace('\"','')
				sql1 = "INSERT OR IGNORE INTO Reads (Taxon, " + patient + ", Kingdom) VALUES (?, ?, ?)"
				cursor.execute(sql1, (OTU_tax, OTU_size, OTU_taxon))
				sql2 = "UPDATE Reads SET " + patient + " = ? WHERE Taxon = ?"
				cursor.execute(sql2, (OTU_size, OTU_tax))

import csv
cursor.execute('SELECT * FROM Reads')
csv_writer = csv.writer(open("c:\Sync Folder\Biorg\Python\Kingdom_Table.csv", "wt"))
csv_writer.writerow([i[0] for i in cursor.description]) # write headers
csv_writer.writerows(cursor)
del csv_writer # this will close the CSV file
cursor.close()
