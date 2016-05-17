#!/usr/bin/python
import sys as sys
from operator import itemgetter
from Workload import Workload
from BatchScheduler import BatchScheduler
from Job import Job
from DAG_Generator import DAG_Generator
from CSV_Creator import CSV_Creator


#Workload
if len(sys.argv) == 1:
	print "Please specify a workload file"
	sys.exit()
else:
	print " "
	print sys.argv
	print "#####     Building workload     #####"
	workload = Workload(sys.argv[1])


print 'file = ', workload.filename
workload.job_list_creator()

#DAG
graph = DAG_Generator(64)


#Batch Scheduler and jobs 
completed_jobs = []
jobs = []
system_procs = 240
batchscheduler = BatchScheduler(system_procs, graph)

print " "
print "#####     Starting simulation     #####"
##########     SIMULATION     ####################

#loop to traverse workload jobs
begin = 0
end = len(workload.workload)-2
for i in range(begin, end):
#while i < len(workload.workload):

	
	job_to_submit = workload.workload.pop(0)
	

    ##########     checks for incorrect jobs in .swf file     ##########
	if job_to_submit.submit_time == -1 or job_to_submit.actual_time == -1 or job_to_submit.number_of_procs == -1:
		#job_to_submit.submit_time = 0
		job_to_submit.actual_time = 1
		job_to_submit.number_of_procs = 1

	if job_to_submit.number_of_procs <= system_procs:
		batchscheduler.submit_new_job(job_to_submit)

	###########     get the time for the next workload job submission     ##########
	time_of_next_job_submission = workload.workload[0].submit_time


	##########     Get the completed jobs before the time of the next job submission     ##########
	while len(batchscheduler.running_queue) > 0 and time_of_next_job_submission >= batchscheduler.running_queue[0].run_queue_end:

		completed_job_from_queue = batchscheduler.running_queue.pop(0)
		batchscheduler.current_time = completed_job_from_queue.run_queue_end

	
		##########     Update variables     ##########
		completed_jobs.append(completed_job_from_queue)
		batchscheduler.number_of_procs_available = batchscheduler.number_of_procs_available + completed_job_from_queue.number_of_procs


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
	

		batchscheduler.FCFS(-1)			
		batchscheduler.backfill(-1) 

	##########     Moves the time forward the appropriate amount     #########
	time_pad = time_of_next_job_submission - batchscheduler.current_time
	batchscheduler.time_advance(time_pad)
	


completed_jobs.sort(key=lambda x: x.run_queue_end)




# results = CSV_Creator(completed_jobs)
# results.results_to_file()


#head_tail_list = []
###### print the jobs ###########
print 'completed_jobs', len(completed_jobs)
# for i in completed_jobs:
# 	wait_time = i.run_queue_start - i.submit_time
# 	run_time = i.run_queue_end - i.run_queue_start
# 	#print i.run_queue_start, i.submit_time
#  	print i.job_id, i.submit_time, wait_time, run_time, i.number_of_procs

	#if i.job.job_id < 0:
#	if i.job.job_id == head_job.job_id:
#		head_tail_list.append(i.job)
#		print 'completed job id:', i.job.job_id
#		print 'head job submit time', i.job.submit_time
#		print
#	if i.job.job_id == tail_job.job_id:
#		head_tail_list.append(i.job)
#		print 'completed job id:', i.job.job_id
#		print 'tail job end time', i.job.run_queue_end
	


print 'program terminates at: ', batchscheduler.current_time
