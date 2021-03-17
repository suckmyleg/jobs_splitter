from random import randint as rd
from threading import Thread as Th
from time import sleep
from json import loads

class jobs_splitter:
	def __init__(self, threads=False, delete_job=True, debug=False, auto_thread=False, interval_log=1, log=False):
		if not threads:
			threads = 1

		self.auto_thread = auto_thread
		self.threads = threads
		self.running_threads = []
		self.jobs = {}
		self.delete_job = delete_job
		self.debug = debug
		self.log = log
		self.interval_log = interval_log
		self.threading_info = loads(open("data.json").read())

	def get_new_job_id(self):
		while True:
			id = str(rd(0, 999))

			try:
				p = self.jobs[id]
			except:
				return id

	def display_info_from_job(self, job_id):
		info = self.jobs[job_id]
		print({"workers_running": info["workers_running"], "status":info["status"], "workers":info["workers"]})

	def get_jobs_ids(self):
		return self.jobs.keys()

	def info(self, job_idd=False):
		if not job_idd:
			jobs_ids = []
		else:
			jobs_ids = [job_id]

		print("Loging: {}".format(jobs_ids))


		while True:
			if not job_idd:
				jobs_ids = self.get_jobs_ids()
			print("")
			for job_id in jobs_ids:
				try:
					self.display_info_from_job(job_id)

					#print(self.jobs)
					"""
					for t in self.jobs[job_id]["workers_status"]:
						print(t)
					print("")"""
				except:
					return True
			sleep(self.interval_log)

	def wait_until(self, job_id, status):
		while True:
			try:
				now_status = self.jobs[job_id]["status"]
			except:
				return True

			if now_status == status:
				break
			sleep(0.001)

	def display_worker_info(self, worker_id, job_id, job, element=False, error=False):
		print("Jobs_splitter: \n--Error: {}\n--Worker_id: {}\n--Job_id: {}\n--Job: {}".format(error, worker_id, job_id, job))
		if element:
			print("--Element: {}".format(element))

	def stop_worker(self, job_id):
		self.jobs[job_id]["workers_running"] -= 1

	def worker(self, job, elements, job_id, worker_id):
		try:
			self.jobs[job_id]["workers_running"] += 1
			self.wait_until(job_id, 1)
			for e in elements:
				try:
					r = job(e)
				except Exception as error:
					r = None
					self.display_worker_info(worker_id, job_id, job, error=e)
				if not r == None:
					self.jobs[job_id]["values"][worker_id].append(r)
		except Exception as e:
			print("Fatal error with worker:")
			self.display_worker_info(worker_id, job_id, job, error=e)
		
		self.stop_worker(job_id)


	def reload_status(self, job_id):
		while True:
			status = self.get_status(job_id)
			self.jobs[job_id]["status"] = status
			if status == 0:
				break

	def get_thread_n_by_elements(self, n):
		try:
			inf = self.threading_info[n-1]
		except:
			inf = self.threading_info[len(self.threading_info)-1]

		slower = [False, False]

		for n in inf[1]:
			if not slower[0] or slower[0] > n[1]:
				slower = [n[1], n[0]]

		print("Slower_option: ", slower)

		return slower[1]

	def round(self, n):
		nn = int(n)
		if n-nn > 0:
			n += 1
		return n

	def get_number_of_workers_needed(self, threads, n_elements):
		r = threads - n_elements
		if r > 0:
			threads -= r
		return threads

	def split_elements(self, all_elements, n):

		lenel = len(all_elements)

		n_times = int(lenel/n)

		if lenel%n > 0:
			n_times += 1

		n_workers = self.get_number_of_workers_needed(n, lenel)

		elements_splitted = []

		print(lenel, n_workers)

		for i in range(n_workers):
			elements_splitted.append([all_elements[a+i*(n_times)] for a in range(lenel-i*(n_times))])

		return elements_splitted, n_workers

	def start_reloading_status(self, job_id):

		t = Th(target=self.reload_status, args=(job_id, ))
		t.start()
		self.running_threads.append(t)

	def create_new_job(self, n):
		id = self.get_new_job_id()

		self.jobs[id] = {"values":[[] for a in range(n)], "workers_running":0, "status":2, "workers":n}

		return id

	def setup_wokers(self, elements_splitted, job, job_id):
		if self.debug:
			print("Setuping workers")
		worker_id = 0
		for i in elements_splitted:
			t = Th(target=self.worker, args=(job, i, job_id, worker_id))
			t.start()
			self.running_threads.append(t)
			worker_id += 1
		if self.debug:
			print("Finished setuping workers")

	def start_log(self, interval_log=False, job_id=False):
		if not interval_log:
			interval_log = self.interval_log
		print("Loging every {} seconds".format(interval_log))
		t = Th(target=self.info, args=(job_id, ))
		t.start()
		self.running_threads.append(t)

	def split_job(self, job, elements, n=False):
		lenel = len(elements)

		if lenel == 0:
			return []

		if self.auto_thread:
			n = self.get_thread_n_by_elements(lenel)
		if not n:
			n = self.threads
 
		elements_splitted, n_workers = self.split_elements(elements, n)

		job_id = self.create_new_job(n_workers)

		self.start_reloading_status(job_id)

		if self.log:
			self.start_log(self.interval_log, job_id)

		if self.debug:
			ll = len(elements_splitted)
			print("Starting job: {} - Workers: {}".format(job_id, ll))

		self.setup_wokers(elements_splitted, job, job_id)

		self.wait_until(job_id, 0)

		result = []

		for m in self.jobs[job_id]["values"]:
			result += m

		if self.delete_job:
			del self.jobs[job_id]

		return result

	def get_status(self, id):
		if self.jobs[id]["workers_running"] == self.jobs[id]["workers"] or not self.jobs[id]["workers_running"] == 0:
			return 1
		else:
			return self.jobs[id]["status"]

			else:
				return 2
