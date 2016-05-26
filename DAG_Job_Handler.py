#!/usr/bin/python
from Job import Job
import networkx as nx
import sys as sys

class DAG_Job_Handler(object):

	def __init__(self, DAG):
		self.DAG = DAG
		self.completed_dag_jobs = []
		self.is_head_job_submitted = True
		self.dag_jobs_in_system = []