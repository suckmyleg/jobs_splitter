from __init__ import *

from time import sleep, time

from json import dumps

m = jobs_splitter(10, debug=False, auto_thread=False)

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
		m.split_job(function_test, t)
		data.append([threads, time()-a])
	return data

elements = 200

data = []
for p in range(elements):

	inf = test(50, p+1)

	data.append([p+1, inf])

for d in data:
	print(d)

open("data.json", "w").write(dumps(data))

input()
