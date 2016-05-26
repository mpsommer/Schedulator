#!/usr/bin/python
import copy

#########################################
#########################################

#####     TODO:
#####     update total jobs in system 1/25/16

#########################################
#########################################

class BatchScheduler(object):

	#initialize a batch scheduler with the amount of processors some system has
	def __init__(self, number_of_procs_in_system):
		object.__init__(self)
		self.number_of_procs_in_system = number_of_procs_in_system
		self.number_of_procs_available = self.number_of_procs_in_system
		self.current_time = 0
		self.total_jobs_in_system = []
		self.waiting_queue = []
		self.running_queue = []
		self.prediction_queue = []
		self.prediction_running_queue = []
	

	def submit_new_job(self, job_to_submit):		
		self.total_jobs_in_system.append(job_to_submit)
		self.current_time = job_to_submit.submit_time	
		self.waiting_queue.append(job_to_submit)
		self.FCFS(job_to_submit.job_id)
		self.backfill(job_to_submit.job_id)
	

	def time_advance(self, duration):
		self.current_time = self.current_time + duration


	#####     Places job in run queue if resources are available essentially FCFS
	def FCFS(self, x):
		while len(self.waiting_queue) > 0 and self.number_of_procs_available >= self.waiting_queue[0].number_of_procs:
			self.waiting_queue[0].run_queue_start = self.current_time
			self.waiting_queue[0].run_queue_end = self.waiting_queue[0].run_queue_start + self.waiting_queue[0].actual_time
			self.waiting_queue[0].sys_procs_avail_at_run_queue_start = self.number_of_procs_available
			self.number_of_procs_available = self.number_of_procs_available - self.waiting_queue[0].number_of_procs
			self.running_queue.append(self.waiting_queue.pop(0))
		self.running_queue.sort(key=lambda x: x.run_queue_end)



	#####     Traverses the waiting list and attempts to backfill all waiting jobs
	#####     function is called when a job ends
	def backfill(self, x):
		self.calculate_job_runqueue_start_and_procs_avail_at_start()
		for job in self.waiting_queue:
			if self.can_conservative_backfill(job):
				job.run_queue_start = self.current_time
				job.sys_procs_avail_at_run_queue_start = self.number_of_procs_available
				job.run_queue_end = job.run_queue_start + job.actual_time
				self.number_of_procs_available = self.number_of_procs_available - job.number_of_procs
				self.running_queue.append(self.waiting_queue.pop(self.waiting_queue.index(job)))
				self.running_queue.sort(key=lambda x: x.run_queue_end)
				self.calculate_job_runqueue_start_and_procs_avail_at_start()
				

	#####     Compare one waiting job against all waiting jobs 
	#####     Checks all jobs in waiting queue that will start before job ends
	#####     If resources available for all jobs with job to backfill running, return true   
	def can_conservative_backfill(self, job):
		should_backfill = True
		if job.number_of_procs > self.number_of_procs_available:
			should_backfill = False
		else:
			job_to_backfill_endtime = self.current_time + job.requested_time
			i = 0
			while i < len(self.waiting_queue) and job_to_backfill_endtime >= self.waiting_queue[i].run_queue_start and should_backfill: 	
				if self.waiting_queue[i].sys_procs_avail_at_run_queue_start - job.number_of_procs < self.waiting_queue[i].number_of_procs and self.waiting_queue[i].job_id != job.job_id:# and self.waiting_queue[i].number_of_procs != job.number_of_procs and self.waiting_queue[i].run_queue_start != job.run_queue_start:
					should_backfill = False
				i = i +1
		return should_backfill



	#####     calculates the run queue start time and the available processes at that time for a given job			
	def calculate_job_runqueue_start_and_procs_avail_at_start(self):	
		self.prediction_queue = list(self.running_queue)
		self.prediction_queue.sort(key=lambda x: x.run_queue_start +x.requested_time)
		should_continue = True
		procs = self.number_of_procs_available
		time = 0
		for job in self.waiting_queue:
			index = self.waiting_queue.index(job)
			if index == 0:
					i = 0
					while i < len(self.prediction_queue) and should_continue:
						procs = procs + self.prediction_queue[i].number_of_procs
						if procs >= job.number_of_procs:
							time = self.prediction_queue[i].run_queue_start + self.prediction_queue[i].requested_time
							i = i +1
							while i < len(self.prediction_queue) and self.prediction_queue[i].run_queue_start + self.prediction_queue[i].requested_time == time:
								procs = procs + self.prediction_queue[i].number_of_procs
								i = i+1
							should_continue = False
						i = i +1
					job.sys_procs_avail_at_run_queue_start = procs
					job.run_queue_start = time 
					self.prediction_queue.append(job)
					self.prediction_queue.sort(key=lambda x: x.run_queue_start +x.requested_time)
			else:
				procs = self.waiting_queue[index-1].sys_procs_avail_at_run_queue_start - self.waiting_queue[index-1].number_of_procs
				if procs >= job.number_of_procs:
					job.sys_procs_avail_at_run_queue_start = procs
					job.run_queue_start = self.waiting_queue[index-1].run_queue_start
					self.prediction_queue.append(job)
					self.prediction_queue.sort(key=lambda x: x.run_queue_start +x.requested_time)
				else:
					i = 0
					should_continue = True
					while i < len(self.prediction_queue) and should_continue:
						if self.prediction_queue[i].run_queue_start + self.prediction_queue[i].requested_time > self.waiting_queue[index-1].run_queue_start:
							procs = procs + self.prediction_queue[i].number_of_procs 
							if procs >= job.number_of_procs:
								time = self.prediction_queue[i].run_queue_start + self.prediction_queue[i].requested_time
								i = i +1
								while i < len(self.prediction_queue) and self.prediction_queue[i].run_queue_start + self.prediction_queue[i].requested_time == time:
									procs = procs + self.prediction_queue[i].number_of_procs 
									i = i +1
								should_continue = False
						i = i +1

					job.sys_procs_avail_at_run_queue_start = procs
					job.run_queue_start = time 
					self.prediction_queue.append(job)
					self.prediction_queue.sort(key=lambda x: x.run_queue_start +x.requested_time)



	def print_running_queue(self):
		while not self.running_queue.empty():
			queue_entry = self.running_queue.get()
			if queue_entry.job.job_id < 0:
				print 'running job_id: ', queue_entry.job.job_id, 'actual time', queue_entry.job.actual_time
				print 'submit time ', queue_entry.job.submit_time
				print 'run_queue_start', queue_entry.job.run_queue_start
				print 'number_of_procs', queue_entry.job.number_of_procs
