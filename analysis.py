# Importing the required modules
# Matplotlib is used to plot the graph wherever required
from matplotlib import pyplot as plt
import math

# Opening the various required log files.
g=open("master.logs","r")
f=open("worker.logs","r")
allog=open("Algorithm.logs","r")
wrs=open("worker_st.logs","r")
ws=open("worker_st.logs","r")
wf=open("worker_ft.logs","r")

for i in allog:
    alg=i
for i in wrs:
    i=i.split(" ")
    rr=i[10]
    break
rr=float(rr)
dt={}
ma=0
# Configuring our required graph labels
fig = plt.figure("Visualization of tasks scheduled")
plt.xlabel('Time(Seconds)')
plt.ylabel('No of Tasks Scheduled')
plt.title(alg,y=1)
dt_ft={}
dt_time={}
dj_t={}
ds_1={}
df_1={}
msa=0

#Reading from logs
for i in ws:
    i=i.strip()
    li=i.split(" ")
    tim=float(li[10])
    op=int(li[14])
    if op not in ds_1:
        ds_1[op]=[tim-rr]
    else:
        ds_1[op].append(tim-rr)

#Reading from logs
for i in wf:
    i=i.strip()
    li=i.split(" ")
    tim=float(li[10])
    op=int(li[14])
    if op not in df_1:
        df_1[op]=[tim-rr]
    else:
        df_1[op].append(tim-rr)
    if int(math.ceil(tim-rr))>msa:
        msa=math.ceil(tim-rr)
x2=[]
for i in range(0,msa+1):
    x2.append(i)
cs1=0
cf1=0
y2=[]
b=plt.subplot(111)
b.spines['top'].set_visible(False)  #Removing the top axis
b.spines['right'].set_visible(False)  #Removing the right axis

# Calculating the array which contains the number of tasks scheduled at a given point of time
for i in sorted(df_1):
    for j in x2:
        cs1=0
        cf1=0
        for k in ds_1[i]:
            if j>=k:
                cs1+=1
        for k in df_1[i]:
            if j>=k:
                cf1+=1
        y2.append(cs1-cf1)
    lb="worker - "+str(i)
    b.plot(x2,y2,'*-',label=lb)  # Plotting the required result
    y2=[]

plt.legend(bbox_to_anchor = (0.865,1) ,frameon=False)
plt.show()

# Reading the master logs and computing the job completion times
for j in g:
    j=j.strip()
    li=j.split(" ")
    tim=li[5]
    job_id=li[2]
    if job_id not in dj_t:
        dj_t[job_id]=tim
mean_job=0.0
median_job=0.0
sum_j=0.0
ar_j=[]
for i in dj_t:
    sum_j+=float(dj_t[i])
    ar_j.append(float(dj_t[i]))

# Median Job completion time
if len(ar_j)%2==1:
    median_job=ar_j[len(ar_j)//2]
else:
    median_job = (ar_j[len(ar_j) // 2]+ar_j[(len(ar_j)-1) // 2])/2
mean_job=float(sum_j/len(dj_t))             # Mean Job completion time

# Printing the Job completion time values
print("\n\n###   JOB COMPLETION TIMES   ###")
for i in sorted(dj_t):
    print("Time taken to complete Job "+str(i)+" is : "+str(dj_t[i]))
print("\n Mean Job completion time = "+str(mean_job))
print("\n Median Job completion time = "+str(median_job))

# Calculating the Task Completion time values
for i in f:
    li=i.split(" ")
    tim=li[9]
    tim=float(tim)
    machine=li[14]
    machine=int(machine)
    if machine not in dt:
        dt[machine]=(tim,1)
    else:
        tt=dt[machine][0]
        cc=dt[machine][1]
        dt[machine]=(tt+tim,cc+1)
    if machine not in dt_time:
        dt_time[machine]=[int(math.ceil(tim))]
        dt_ft[machine]=[tim]
    else:
        dt_time[machine].append(int(math.ceil(tim)))
        dt_ft[machine].append(tim)
    if math.ceil(tim)>ma:
        ma=math.ceil(tim)

for i in dt_time:
    dt_time[i].sort()
ar=[]

# Printing the task completion time values
print("\n\n###   TASK COMPLETION TIMES   ###")

for i in sorted(dt):
    mean=float(dt[i][0]/dt[i][1])
    print("Mean task completion time for worker "+str(i)+" is : "+str(mean)+" seconds")
print()
for i in sorted(dt_ft):
    lty=len(dt_ft[i])
    if lty%2==1:
        median=dt_ft[i][lty//2]
    else:
        mid_pos=lty//2
        median=(dt_ft[i][mid_pos-1]+dt_ft[i][mid_pos])/2
    print("Median Task completion time for worker "+str(i)+" is :"+str(median)+" seconds")
print("\n")
for k in range(0,len(dt_time)):
    ar.append(0)

# Plotting the graph of Number of tasks finished vs time
p=[]
xv=[]
u=[]
label_arr=[]
sr="worker "
for i in range(0, int(ma) + 2):
    xv.append(i)
fig = plt.figure("Visualization of tasks finished")
plt.xlabel('Time(Seconds)')
plt.ylabel('No of Tasks Finished')
plt.title(alg,y=1)
a=plt.subplot(111)
a.spines['top'].set_visible(False)
a.spines['right'].set_visible(False)

y1=[]
# Calculating the array which contains the number of tasks finished at a given point of time
for i in sorted(df_1):
    for j in x2:
        cf2=0
        for k in df_1[i]:
            if j>=k:
                cf2+=1
        y1.append(cf2)
    lb="worker - "+str(i)
    a.plot(x2,y1,'*-',label=lb)
    y1=[]
plt.legend(bbox_to_anchor = (0.3,1) ,frameon=False)

# Closing all the opened log files
plt.show()
g.close()
f.close()
wrs.close()
ws.close()
allog.close()
wf.close()
