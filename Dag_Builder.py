#!/usr/bin/python
from Job import Job
import networkx as nx

class Dag_Builder(object):

	def __init__(self,filename):
		object.__init__(self)
		self.filename = filename
		self.dag = nx.DiGraph()

	def dag_builder(self):
		try:
			dag_file = open(self.filename, 'r')
		except IOError, e:
			print 'No such file or directory:'
			sys.exit()