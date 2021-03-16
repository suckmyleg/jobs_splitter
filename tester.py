from __init__ import *

from time import sleep, time

from json import dumps

m = jobs_splitter(10, debug=False, auto_thread=False, log=True, interval_log=0.1)

def function_test(l):
	sleep(0.5)
	return l

def test(ran, ran2, start_t=0):
	data = []

	t = [0 for kl in range(ran2)]

	for i in range(ran):
		threads = i+1+start_t
		print("Testing with {} threads and {} objects".format(threads, ran2))
		a = time()
		m.split_job(function_test, t, n=i)
		data.append([threads, time()-a])
	return data
def testing():
	elements = 200

	data = []
	for p in range(elements):

		inf = test(10, p+1)

		data = ([p+1, inf])

		#print(m.jobs)

		#for d in data:
			#print(d)


		#open("data.json", "w").write(dumps(data))
		
def function_for_testing(mess):
	#print(mess)#, "Sleeping for 3 seconds")
	sleep(0.01)
	return 1


def show():
	thread = 100
	elements = range(9999)

	m.split_job(function_for_testing, elements, n=thread)

show()

input()
