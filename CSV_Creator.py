#!/usr/bin/python
from time import gmtime, strftime
from Job import Job

class CSV_Creator(object):
	def __init__(self, completed_jobs):
		object.__init__(self)
		self.completed_jobs = completed_jobs
		self.file_name = strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ".csv"


	def results_to_file(self):
		target = open(self.file_name, 'w')
		#target.write("Job id,     arrival,     wait time,     run time,     CPUs \n")
		for i in self.completed_jobs:
			wait_time = i.run_queue_start - i.submit_time
			run_time = i.run_queue_end - i.run_queue_start
			target.write(str(i.job_id) + " ")
			target.write(str(i.submit_time) + " ")
			target.write(str(wait_time) + " ")
			target.write(str(run_time) + " ")
			target.write(str(i.number_of_procs))
			target.write("\n")

		target.close()



