import os, os.path
import re
import csv

def do_parse(dirtocheck, filename):
	fout = open(filename, 'wb')
	fwriter = csv.writer(fout)
	for root, _, files in os.walk(dirtocheck):
		for f in files:
			fullpath = os.path.join(root, f)
			try:
				lines = parse_file(fullpath)
			except StandardError as e:
				print r'In do_parse: ' + fullpath
				lines = []
			for line in lines:
				fwriter.writerow(line)
	fout.close()

def parse_file(fullpath):
	fin = open(fullpath, 'r')
	data = fin.read()
	fin.close()
	
	try:
		fmt = r'(?s)<(SEC|IMS)-HEADER>(.*)</(SEC|IMS)-HEADER>'
		header = re.findall(fmt,data,re.DOTALL)[0][1]
	except StandardError as e:
		header=r''
	
	fmt = r'(?s)<DOCUMENT>(.*?)</DOCUMENT>'
	bodies = re.findall(fmt,data,re.DOTALL)
	
	for part in bodies:
		fmt = r'<TYPE>NSAR'
		if len(re.findall(fmt,part))>0:
			body = part
			break
	
	# Got body and header, now extract values we care about from header:
	try:
		fmt = r'(?sm)ACCESSION NUMBER:\s+(\S+)\s*$'
		accession_num =  re.findall(fmt,header)[0]
	except StandardError as e:
		accession_num=r''
	
	try:
		fmt = r'(?sm)CENTRAL INDEX KEY:\s+(\d+)\s*$'
		field_cik =  re.findall(fmt,header)[0]
	except StandardError as e:
		field_cik=r''
	
	try:
		fmt = r'(?sm)^\s*<SERIES>\s*$(.*?)^\s*</SERIES>\s*$'
		series =  re.findall(fmt,header)
	except StandardError as e:
		series=[]
	
	series_data=[]
	for part in series:
		try:
			fmt = r'(?sm)^\s*<SERIES-NAME>\s*(.*?)\s*$'
			series_name = re.findall(fmt,part)[0]
		except StandardError as e:
			series_name=r''
		
		try:
			fmt = r'(?sm)^\s*<CLASS-CONTRACT-TICKER-SYMBOL>\s*(.*?)\s*$'
			tickers = re.findall(fmt,part)
		except StandardError as e:
			tickers=[r'']
		
		series_data.append((series_name,tickers))
	
	# Next, tidy body data
	fmt = r'(?sm)^(000.*)^SIGNATURE\s*(.*?)\s*$\s*TITLE[\t ]*(.*?)\s*$'
	temp = re.findall(fmt,body)
	signed = temp[0][1]
	title = temp[0][2]
	temp = temp[0][0]
	temp = re.sub(r'(?sm)^<PAGE>.*?$', r'\n', temp)
	temp = re.sub(r'\n\s*\n', r'\n', temp)
	temp = re.sub(r'\n\s*\n', r'\n', temp)
	body = re.sub(r'(?sm)^"(.*?)"\s*$', r'\1', temp)
	
	baseline = (accession_num, field_cik, signed)
	ln = Lines(baseline, series_data, body, fullpath)
	return ln.getLines()

class Lines:
	def __init__(self, baseline, series, body, fullpath):
		self.fullpath = fullpath
		self.baseline = baseline
		if series==[]:
			series = [('',[''])]
		self.series = series
		self.Q1 = self.getQ1(body)
		self.Q28 = self.getQ28(body)
		self.Q7 = self.getQ7(body)

	def getQ1(self, body):
		Q = []
		Q.append(self.parseQ(body, r'000', r' C00')[0][1])
		Q.append(self.parseQ(body, r'000', r' A00')[0][1])
		Q.append(self.parseQ(body, r'000', r' B00')[0][1])
		Q.append(self.parseQ(body, r'001', r' A00')[0][1])
		Q.append(self.parseQ(body, r'087', r' A01')[0][1])
		Q.append(self.parseQ(body, r'087', r' A02')[0][1])
		Q.append(self.parseQ(body, r'087', r' A03')[0][1])
		return Q

	def getQ28(self, body):
		Q = []
		Q.append(self.parseQ(body, r'028', r' A01'))
		Q.append(self.parseQ(body, r'028', r' A02'))
		Q.append(self.parseQ(body, r'028', r' A03'))
		Q.append(self.parseQ(body, r'028', r' A04'))
		Q.append(self.parseQ(body, r'028', r' B01'))
		Q.append(self.parseQ(body, r'028', r' B02'))
		Q.append(self.parseQ(body, r'028', r' B03'))
		Q.append(self.parseQ(body, r'028', r' B04'))
		Q.append(self.parseQ(body, r'028', r' C01'))
		Q.append(self.parseQ(body, r'028', r' C02'))
		Q.append(self.parseQ(body, r'028', r' C03'))
		Q.append(self.parseQ(body, r'028', r' C04'))
		Q.append(self.parseQ(body, r'028', r' D01'))
		Q.append(self.parseQ(body, r'028', r' D02'))
		Q.append(self.parseQ(body, r'028', r' D03'))
		Q.append(self.parseQ(body, r'028', r' D04'))
		Q.append(self.parseQ(body, r'028', r' E01'))
		Q.append(self.parseQ(body, r'028', r' E02'))
		Q.append(self.parseQ(body, r'028', r' E03'))
		Q.append(self.parseQ(body, r'028', r' E04'))
		Q.append(self.parseQ(body, r'028', r' F01'))
		Q.append(self.parseQ(body, r'028', r' F02'))
		Q.append(self.parseQ(body, r'028', r' F03'))
		Q.append(self.parseQ(body, r'028', r' F04'))
		Q.append(self.parseQ(body, r'028', r' G01'))
		Q.append(self.parseQ(body, r'028', r' G02'))
		Q.append(self.parseQ(body, r'028', r' G03'))
		Q.append(self.parseQ(body, r'028', r' G04'))
		Q.append(self.parseQ(body, r'028', r' H00'))
		return Q

	def getQ7(self, body):
		Q = []
		Q.append(self.parseQ(body, r'007', r' C01'))
		Q.append(self.parseQ(body, r'007', r' C02'))
		Q.append(self.parseQ(body, r'007', r' C03'))
		return Q

	def __del__(self):
		pass

	def getLines(self):
		#Fixup seperate lines from all the data collected
		rep = len(self.Q28[0])
		a,b,c = self.baseline
		if (rep==1 and len(self.series)==1):
			#single line to be produced
			if (self.series[0][1] != ['']):
				d = tuple(self.series[0][1])
				base = (a,b,c,d,'')  #last '' is for Q7 data which we don't add here
			else:
				base = (a,b,c,'','')
			base = base + tuple(self.Q1) + tuple(self.parse_Q28(0,-1))
			return [base]
		else:
			#multiple (rep) lines to be produced
			bases=[]
			use_series=1
			if len(self.series)!=rep:
				use_series=0
			for i in range(0,rep):
				e, curr_id = self.parse_Q7(i,rep)
				if (use_series and self.series[i][1] != ['']):
					d = tuple(self.series[i][1])
					base = (a,b,c,d,e)  
				else:
					base = (a,b,c,'',e)
				base = base + tuple(self.Q1) + tuple(self.parse_Q28(i, curr_id))
				bases.append(base)
			return bases

	def parse_Q28(self, cnt, curr_id):
		ret = []
		err_print = 0
		for val in self.Q28:
			try:
				if (val[cnt][0]==r'AA'):
					cnt=0
					cnt2=0
				else:
					cnt2 = int(val[cnt][0])
			except StandardError as e:
				cnt2 = 0
			if ((cnt2==0 and cnt!=0) or (cnt2!=0 and curr_id!=-1 and cnt2!=curr_id)):
				if err_print==0:
					#print "Error in parse_Q28"
					#print self.fullpath
					err_print=1
					ret.append('')
				else:
					ret.append('')
			else:
				ret.append(val[cnt][1])
		return ret
	
	def parse_Q7(self, cnt, rep):
		try:
			cnt2 = int(self.Q7[1][cnt][0])
		except StandardError as e:
			cnt2 = 0
		if (cnt2==0):
			#print "Error in parse_Q7"
			#print self.fullpath
			return ('',cnt2)
		return (self.Q7[1][cnt][1], cnt2)
	
	def parseQ(self, data, qnum, subq):
		fmt = r'(?sm)^' + qnum + subq + r'(..)..\s*(.*?)\s*$'
		tmp = re.findall(fmt,data)
		if tmp==[]:
			return [('','')]
		else:
			return tmp

			

# ######################################################################
# Execution
# ######################################################################

print "Starting NSAR Parsing"

do_parse(r'C:\DATA\Kaniel\NSAR\wget\NSARB',r'C:\DATA\Kaniel\NSAR\NSARBres.csv') 
#lines = parse_file(r'C:\DATA\Kaniel\NSAR\Test\0000002768-04-000007.txt')
#print lines

print "Ending NSAR Parsing"


