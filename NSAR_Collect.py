import sqlite3
import os, os.path
import struct

def init(cursor):
	# create a table
	try:
		sql = """
		CREATE TABLE filings      ( 
				   form_type      , 
				   company_name   , 
				   cik            , 
				   date_filed     , 
				   path_to_filing , 
				   sourcefile     )
		"""
		cursor.execute(sql)
	except sqlite3.OperationalError:
		sql = """
		DROP TABLE filings
		"""
		cursor.execute(sql)
		sql = """
		CREATE TABLE filings      ( 
				   form_type      , 
				   company_name   , 
				   cik            , 
				   date_filed     , 
				   path_to_filing , 
				   sourcefile     )
		"""
		cursor.execute(sql)

	dirtocheck = r'C:\DATA\Kaniel\NSAR\wget\ftp.sec.gov\edgar\daily-index'
	for root, _, files in os.walk(dirtocheck):
		for f in files:
			fullpath = os.path.join(root, f)
			add_sql(cursor, fullpath)


def add_sql(cursor, fullpath):
	fieldwidths = (12, 62, 12, 12, 43)
	fmtstring = ''.join('%ds' % f for f in fieldwidths)
	parse = struct.Struct(fmtstring).unpack_from
	found_header = 0;
	for line in open(fullpath,'r'):
		if line.find(r'--------------------------------------') != -1:
			found_header=1;
			continue
		if found_header==0:
			continue
		if len(line)<141:
			line = line + '          '
		a,b,c,d,e = parse(line)
		fields = a.strip(),b.strip(),c.strip(),d.strip(),e.strip(),fullpath.strip()
		try:
			cursor.execute('INSERT INTO filings VALUES (?,?,?,?,?,?)', fields)
		except sqlite3.Error as e:
			print "An error occurred:", e.args[0], " at: ", fullpath, " line: ", line


def getfile(filepath,fout):
	filepath = filepath[0]
	if filepath.find(r'edgar') == -1:
		filepath = r'ftp://ftp.sec.gov/edgar/' + filepath
	else:
		filepath = r'ftp://ftp.sec.gov/' + filepath
	fout.write("%s\n" % filepath)



# ######################################################################
# Execution
# ######################################################################

print "Starting NSAR Data Collection"
 
# start s db
conn = sqlite3.connect(r'C:\DATA\Kaniel\NSAR\forms.db') # or use :memory: to put it in RAM
cursor = conn.cursor()

# initialize it with all the index data from EDGAR
#init(cursor) 
#conn.commit()

# get all rows with NSAR-XX type, and write url to file (passed to wget)
#fout = open(r'C:\Data\Kaniel\NSAR\NSARBT.txt', 'w')
#sql = """
#SELECT DISTINCT path_to_filing 
#      FROM filings 
#      WHERE form_type='NSAR-BT'
#"""
#for filepath in cursor.execute(sql):
#	getfile(filepath,fout)
#fout.close()

conn.close()


print "Ending NSAR Data Collection"


