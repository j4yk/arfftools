#!/usr/bin/python
import os.path
import cPickle as pickle

class Attribute:
	NOMINAL = 'nominal'
	NUMERIC = 'numeric'
	def __init__(self, name):
		self.name = name
		self.type = self.NUMERIC
		self.values = set()
	
	def covers_value(self, value):
		if '"' in value or '%' in value:
			self.values.add('"' + value + '"')
			self.type = self.NOMINAL
		else:
			self.values.add(value)
		if self.type != self.NOMINAL:
			try:
				number = float(value)
			except ValueError:
				self.type = self.NOMINAL

	def typedecl(self):
		if self.type == self.NOMINAL:
			return '{' + ','.join(self.values) + '}'
		else:
			return self.type

class Csv2ArffConverter:
	BATCH_SIZE = 100 * 2**20
	def __init__(self, csv_file, arff_file, separator=','):
		self.relation_name = os.path.basename(csv_file)
		self.csv_file = csv_file
		self.arff_file = arff_file
		self.arff_header_file = arff_file + '.head'
		self.arff_header_file_exists = os.path.exists(self.arff_header_file)
		self.separator = separator
		self.attributes = []
	
	def __enter__(self):
		self.csv_file = open(self.csv_file, 'r')
		self.arff_file = open(self.arff_file, 'w')
		return self

	def __exit__(self, type, value, traceback):
		self.csv_file.close()
		self.arff_file.close()
	
	def run(self):
		self.gather_attributes()
		self.write_header()
		self.convert_data()
	
	def gather_attributes(self):
		if self.arff_header_file_exists:
			with open(self.arff_header_file, 'r') as f:
				self.attributes, self.instanceCount = pickle.load(f)
			self.csv_file.readline() # skip headline
			return
		headline = self.csv_file.readline()
		attributeNames = headline.strip("\r\n").split(self.separator)
		for attributeName in attributeNames:
			self.attributes.append(Attribute(attributeName))
		sys.stderr.write("Reading lines to gather attribute values\n0")
		self.instanceCount = 0
		while True:
			lines = self.csv_file.readlines(self.BATCH_SIZE)
			if not lines:
				break
			for line in lines:
				self.instanceCount += 1
				if self.instanceCount % 100 == 0:
					sys.stderr.write("\r%d" % self.instanceCount)
				values = line.strip("\r\n").split(self.separator)
				if len(values) != len(self.attributes):
					raise Exception(
							"Wrong number of columns (%d instead of %d) on the following line: %s"
							% (len(values), len(self.attributes), line))
				attributeIndex = 0
				for value in values:
					self.attributes[attributeIndex].covers_value(value)
					attributeIndex += 1
		self.csv_file.seek(0)
		self.csv_file.readline() # skip headline
		with open(self.arff_header_file, 'w') as f:
			pickle.dump((self.attributes, self.instanceCount), f)
	
	def write_header(self):
		sys.stderr.write("\rWriting header\n")
		self.arff_file.write('@RELATION "' + self.relation_name + "\"\n\n")
		for attribute in self.attributes:
			self.arff_file.write('@ATTRIBUTE ' + attribute.name + ' '
					+ attribute.typedecl() + "\n")
		self.arff_file.write("\n")
	
	def convert_data(self):
		sys.stderr.write("Converting instances\n0 %")
		self.arff_file.write("@DATA\n")
		lineCount = 0
		while True:
			lines = self.csv_file.readlines(self.BATCH_SIZE)
			if not lines:
				break
			for line in lines:
				values = (self.escape(value) for value in line.strip("\r\n").split(self.separator))
				self.arff_file.write(','.join(values) + "\n")
				lineCount += 1
				if lineCount % (self.instanceCount // 100) == 0:
					sys.stderr.write("\r%d %% " % ((100 * lineCount) // self.instanceCount))
				if lineCount % 1000 == 0:
					sys.stderr.write('.')
		sys.stderr.write("\rdone\n")
	
	def escape(self, value):
		if ',' in value or '%' in value:
			return '"' + value + '"'
		else:
			return value

if __name__ == '__main__':
	import sys
	with Csv2ArffConverter(sys.argv[1], sys.argv[2]) as converter:
		converter.run()
