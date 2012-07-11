import sys

def split_line(line):
    result = dict()
    for token in line.split('\t'):
        index = token.find('=')
        key = token[:index]
        value = token[index + 1:]
        result[key] = value
    return result

def tabular_input(stream = sys.stdin):
    for line in stream:
        if line[-1] == '\n':
            line = line[:-1]
        yield split_line(line)

class TabularOutput:
	def __init__(self, stream):
		self.stream = stream

	def write(self, row):
		count = 0
		for key in row:
			self.stream.write(key)
			self.stream.write('=')
			self.stream.write(str(row[key]))
			count += 1
			if count < len(row):
				self.stream.write('\t')
		self.stream.write('\n')

def tabular_output(stream = sys.stdout):
	return TabularOutput(stream)

class GroupIterator:
	def __init__(self, owner, key):
		self.owner = owner
		self.key = key

	def __iter__(self):
		return self

	def next(self):
		row = self.owner.peek_row()
		if row[self.owner.key_column] == self.key:
			self.owner.skip_row()
			return row
		self.owner.group_finished()
		raise StopIteration()	


class GroupsIterator:
	def __init__(self, rows, key_column):
		self.rows = rows
		self.key_column = key_column
		self.cur_row = None
		self.cur_key = None

	def __iter__(self):
		return self

	def peek_row(self):
		if self.cur_row is None:
			self.cur_row = self.rows.next()
		return self.cur_row

	def skip_row(self):
		self.cur_row = None

	def group_finished(self):
		self.cur_key = None

	def next(self):
		while True:
			row = self.peek_row()
			if (self.cur_key is None) or (row[self.key_column] != self.cur_key):
				break
			self.skip_row()
		self.cur_key = row[self.key_column]	
		return (self.cur_key, GroupIterator(self, self.cur_key))


def group_by(rows, key_column):
	return GroupsIterator(rows, key_column)

def run_map(mapper):
	i = tabular_input()
	o = tabular_output()
	for irow in i:
		for orow in mapper(irow):
			o.write(orow)

def run_reduce(reducer, key_column):
	i = tabular_input()
	o = tabular_output()
	for (key, irows) in group_by(i, key_column):
		for orow in reducer(key, irows):
			o.write(orow)
