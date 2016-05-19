#!/usr/bin/python
from Job import Job
import networkx as nx
import sys as sys
# import matplotlib.pyplot as plt

class DAG_Generator(object):


	def __init__(self, config_file):
		object.__init__(self)
		self.DAG = nx.DiGraph()
		self.filename = config_file
		self.dag_creator()



	def dag_creator(self):
		try:
			DAG_config_file = open(self.filename, 'r')
		except IOError, e:
			print 'No such DAG config file'
			sys.exit()
		task_list = []
		for (i,line) in enumerate(DAG_config_file.readlines()):
			line = line.split()
			if line[0].startswith('t'):
				task_list.append(Job(long(line[1]), 0, long(line[2]), (line[3]), line[4], 0, 0, 0, True))	
			elif line[0].startswith('e'):
				x = int(line[1])
				y = int(line[2])
				self.DAG.add_edge(task_list[x], task_list[y])

		# nx.draw(self.DAG)
		# plt.savefig("simple_path.png") # save as png
		# plt.show() # display
		return self.DAG


















