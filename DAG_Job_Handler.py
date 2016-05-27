#!/usr/bin/python
from Job import Job
import networkx as nx
import sys as sys

class DAG_Job_Handler(object):

	def __init__(self, DAG):
		self.DAG = DAG
		self.completed_dag_jobs = []
		self.is_head_job_submitted = False
		self.dag_jobs_in_system = []

	def get_head_node(self):
		for job in self.DAG:
			if not self.DAG.predecessors(x):
				return job


		# ##########     submit first dag job     ##########
		# if batchscheduler.current_time >= 1000000 and batchscheduler.is_head_job_submitted == True:
		# 	x = batchscheduler.graph.get_head_node()
		# 	x.submit_time = batchscheduler.current_time
		# 	batchscheduler.submit_new_job(x)
		# 	batchscheduler.dag_jobs_in_system.append(x)
		# 	batchscheduler.is_head_job_submitted = False

		# ##submit completed dag jobs to a list
		# count = 0
		# if completed_job_from_queue.is_dag_job:
		# 	batchscheduler.completed_dag_jobs.append(completed_job_from_queue)

		# 	#########     Logic to submit the dag jobs dependent on completed dag job     ##########
		# 	successors_list = batchscheduler.DAG.successors(completed_job_from_queue)
		# 	for x in successors_list:
		# 		predecessors_list = batchscheduler.DAG.predecessors(x)
		# 		count = 0
		# 		for y in predecessors_list:
		# 			if y in batchscheduler.completed_dag_jobs:
		# 				count = count + 1
		# 		if count == batchscheduler.DAG.in_degree(x):
		# 			x.submit_time = batchscheduler.current_time
		# 			batchscheduler.submit_new_job(x)
		# 			batchscheduler.dag_jobs_in_system.append(x)