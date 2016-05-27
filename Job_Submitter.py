#!/usr/bin/python
import sys as sys
from operator import itemgetter
from Workload import Workload
from BatchScheduler import BatchScheduler
from Job import Job
from DAG_Handler import DAG_Handler
from CSV_Creator import CSV_Creator

if len(sys.argv) != 5:
	print " "
	print "Please specify a workload file, DAG config file and the first DAG job submit time"
	print " "
	sys.exit()


workload = Workload(sys.argv[1])
DAG = DAG_Handler(sys.argv[2])
head_job_submit_time = int(sys.argv[3])
system_procs = int(sys.argv[4])


print " "
print 'SWF file =', workload.filename
print 'DAG config file =', DAG.filename
print 'First DAG job submit time =', head_job_submit_time
print 'System processes =', system_procs

print " "
print "        Building workload..."
print " "
workload.job_list_creator()

print " "
print "        Building DAG...     "
print " "
DAG.dag_creator()


batchscheduler = BatchScheduler(system_procs)
completed_jobs = []
#successors_list = []
jobs = []

print " "
print "        Starting simulation...     "
print " "

#loop to traverse workload jobs
begin = 0
end = len(workload.workload)-2
for i in range(begin, end):

	job_to_submit = workload.workload.pop(0)

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
		if batchscheduler.current_time >= head_job_submit_time and DAG.is_head_job_submitted == False:
			head_job = DAG.get_head_node()
			head_job.submit_time = batchscheduler.current_time
			batchscheduler.submit_new_job(head_job)
			DAG.dag_jobs_in_system.append(head_job)
			DAG.is_head_job_submitted = True


		# #########     Logic to submit the dag jobs dependent on completed dag job     ##########
		if completed_job_from_queue.is_dag_job:
			DAG.completed_dag_jobs.append(completed_job_from_queue)
			successors_list = DAG.DAG.successors(completed_job_from_queue)
			for x in successors_list:
				predecessors_list = DAG.DAG.predecessors(x)
				count = 0
				for y in predecessors_list:
					if y in DAG.completed_dag_jobs:
						count = count + 1
				if count == DAG.DAG.in_degree(x):
					x.submit_time = batchscheduler.current_time
					batchscheduler.submit_new_job(x)
					DAG.dag_jobs_in_system.append(x)
	

		batchscheduler.FCFS()			
		batchscheduler.backfill() 

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
