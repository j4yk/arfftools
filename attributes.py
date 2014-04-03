NOMINAL = 'nominal'
NUMERIC = 'numeric'
STRING = 'string'

class UnknownTypeError(Exception):
	pass

class Attribute:
	def __init__(self, name, type=NUMERIC):
		self.name = name
		self.values = set()
		self.type = self._parse_type(type)
	
	def _parse_type(self, type):
		try:
			if type.lower() in (NUMERIC, STRING):
				return type.lower()
			elif type[0] == '{':
				self.values = {value.strip('" ')
						for value in type[1:type.rfind('}')].split(',')}
				return NOMINAL
			else:
				raise UnknownTypeError(type)
		except IndexError:
			raise UnknownTypeError(type)
	
	def covers_value(self, value):
		if '"' in value or '%' in value:
			self.values.add('"' + value + '"')
			self.type = NOMINAL
		else:
			self.values.add(value)
		if self.type != NOMINAL:
			try:
				number = float(value)
			except ValueError:
				self.type = NOMINAL

	def typedecl(self):
		if self.type == NOMINAL:
			return '{' + ','.join(self.values) + '}'
		else:
			return self.type
