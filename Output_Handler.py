#!/usr/bin/python
from time import gmtime, strftime
from datetime import datetime
from Job import Job

class Output_Handler(object):
	def __init__(self):
		object.__init__(self)

	# write job_list attributes to csv file
	def results_to_file(self, job_list):
		file_name = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + ".csv"
		target = open(file_name, 'w')
		target.write("Job id,     submit_time,     wait time,     run time,     num_procs \n")
		for i in job_list:
			wait_time = i.run_queue_start - i.submit_time
			run_time = i.run_queue_end - i.run_queue_start
			target.write(str(i.job_id) + " ")
			target.write(str(i.submit_time) + " ")
			target.write(str(wait_time) + " ")
			target.write(str(run_time) + " ")
			target.write(str(i.number_of_procs))
			target.write("\n")
		target.close()

	# print the job_list attributes
	def results_to_console(self, job_list):
		print "Job id,     submit_time,     wait time,     run time,     num_procs"
		for i in job_list:
			wait_time = i.run_queue_start - i.submit_time
			run_time = i.run_queue_end - i.run_queue_start
			print i.job_id, " ", i.submit_time, " ", wait_time, " ", run_time, " ", i.number_of_procs
