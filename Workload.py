#!/usr/bin/python
from Job import Job
import sys as sys

"""
This class handles the functionality that parses the swf file into some workload and has functionality that returns each entry in the workload as
some job with the required attributes as longs. 
"""

class Workload(object):

	def __init__(self,filename):
		object.__init__(self)
		self.filename = filename
		self.workload = []


    #This function parses and creates a list of strings from the swf file
	def job_list_creator(self):
		try:
			job_list_file = open(self.filename, 'r')
		except IOError, e:
			print 'No such workload file:'
			sys.exit()
		is_dag_job = False
		for (i,line) in enumerate(job_list_file.readlines()):
			#split removes multiple whitespaces, join removes leading and trailing whitespace and joins on ' '
			line = ' '.join(line.split())
			arr = line.split(" ")
			if not line.startswith(';') and line:#     if allocated procs are greater than requested procs, reduce allocated
				if long(arr[4]) > long(arr[7]):
					arr[4] = arr[7]
				if long(arr[8]) < long(arr[3]):#     if requested time is less than actual time, switch
					temp = arr[3]
					arr[3] = arr[8]
					arr[8] = temp
				self.workload.append(Job(long(arr[0]), long(arr[1]), long(arr[8]), long(arr[3]) , long(arr[4]), 0, 0, 0, is_dag_job))
		self.workload.append('-1 100000000000 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 1 -1 -1 -1')
		job_list_file.close()