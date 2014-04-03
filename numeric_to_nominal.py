#!/usr/bin/python
# TODO: add support for -V switch which inverts the attributes selection
import os.path
import sys
import re
import cPickle as pickle
from attributes import Attribute
import attributes

class MalformedInputError(Exception):
	pass

class BadCommandLineArgs(Exception):
	pass

class NumericToNominal:
	BATCH_SIZE = 100 * 2**20
	def __init__(self, attribute_range_string):
		self.attribute_sequences = []
		for sequence in attribute_range_string.split(','):
			ends = sequence.split('-')
			if len(ends) == 1:
				self.attribute_sequences.append(ends)
			else:
				self.attribute_sequences.append((ends[0], ends[1]))
		self.attributes = []
	
	def run(self, in_file, out_file):
		self.in_file = in_file
		self.out_file = out_file
		self.gather_attributes()
		self.write_header()
		self.convert_data()
	
	def gather_attributes(self):
		pickle_tmp = 'tmp.fjasjfasfsfjkfjsjf.pickle'
		try:
			with open(pickle_tmp, 'rb') as f:
				self.relation_name, self.attributes, self.instanceCount = \
						pickle.load(f)
		except IOError:
			pass
		else:
			return
		while True:
			line = self.in_file.readline()
			relation_match = re.search('^@relation ([^%\s]*)', line, re.I)
			if relation_match:
				self.relation_name = relation_match.group(1)
				break
		self.read_attribute_declarations()
		self.expand_attribute_range()
		for index, attribute in enumerate(self.attributes):
			if index in self.attribute_indices:
				attribute.type = attributes.NOMINAL
		sys.stderr.write("Reading lines to gather attribute values\n0")
		self.instanceCount = 0
		while True:
			lines = self.in_file.readlines(self.BATCH_SIZE)
			if not lines:
				break
			for line in lines:
				self.instanceCount += 1
				if self.instanceCount % 1000 == 0:
					sys.stderr.write("\r%d" % self.instanceCount)
				values = line.strip("\r\n").split(',')
				if len(values) == 0:
					continue
				if len(values) != len(self.attributes):
					raise Exception(
							"Wrong number of columns (%d instead of %d) on the following line: %s"
							% (len(values), len(self.attributes), line))
				for index, value in enumerate(values):
					if index in self.attribute_indices:
						self.attributes[index].covers_value(value)
		self.in_file.seek(0)
		with open(pickle_tmp, 'wb') as f:
			pickle.dump(
					(self.relation_name, self.attributes, self.instanceCount),
					f)
	
	def read_attribute_declarations(self):
		while True:
			line = self.in_file.readline()
			try:
				if line.strip().lower() == '@data':
					return
			except AttributeError:
				raise MalformedInputError()
			attribute_match = \
					re.search('^@attribute ([^%\s]+) ([^%]+)$', line, re.I)
			if attribute_match:
				self.attributes.append(Attribute(
						attribute_match.group(1),
						attribute_match.group(2).strip()))
	
	def expand_attribute_range(self):
		self.attribute_indices = []
		for sequence in self.attribute_sequences:
			if sequence[0] == 'first':
				sequence[0] = 1
			if len(sequence) > 1:
				if sequence[1] == 'last':
					sequence[1] = len(self.attributes)
			else:
				sequence = (sequence[0], sequence[0])
			try:
				self.attribute_indices.extend(
						range(int(sequence[0]) - 1, int(sequence[1])))
			except TypeError:
				raise BadCommandLineArgs
		if len(self.attribute_indices) < 1:
			raise BadCommandLineArgs
	
	def write_header(self):
		sys.stderr.write("\rWriting header\n")
		self.out_file.write('@RELATION "' + self.relation_name + "\"\n\n")
		for attribute in self.attributes:
			self.out_file.write('@ATTRIBUTE ' + attribute.name + ' '
					+ attribute.typedecl() + "\n")
		self.out_file.write("\n")
	
	def skip_to_data(self):
		while True:
			line = self.in_file.readline()
			if line.strip().upper() == '@DATA':
				return
	
	def convert_data(self):
		self.skip_to_data()
		sys.stderr.write("Converting instances\n0 %")
		self.out_file.write("@DATA\n")
		lineCount = 0
		while True:
			lines = self.in_file.readlines(self.BATCH_SIZE)
			if not lines:
				break
			self.out_file.writelines(lines)
			lineCount += len(lines)
			if lineCount % (self.instanceCount // 100) < len(lines):
				sys.stderr.write("\r%d %% " % ((100 * lineCount) // self.instanceCount))
			if lineCount % 1000 < len(lines):
				sys.stderr.write('.')
		sys.stderr.write("\rdone\n")

if __name__ == '__main__':
	import argparse, sys
	parser = argparse.ArgumentParser()
	parser.add_argument("-R", dest='convert_attribue_range', nargs='?',
			default='first-last',
			help="attribute range to convert from numeric to nominal (same as "
			"-R for the weka filter)")
	parser.add_argument("input_arff", type=argparse.FileType('r'))
	parser.add_argument("output_arff", nargs='?', type=argparse.FileType('w'),
			default=sys.stdout)
	args = parser.parse_args()
	try:
		NumericToNominal(args.convert_attribue_range)\
				.run(args.input_arff, args.output_arff)
	except BadCommandLineArgs:
		parser.print_help()
