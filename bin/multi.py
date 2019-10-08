
#!/home/ebdptosp/.conda/envs/tos3.6/bin/python3

from datetime import datetime
from datetime import timedelta
from pathlib import Path
import shutil
import glob
import os
import time
import sys
import numpy as np
import multiprocessing
from multiprocessing import Pool
from itertools import product
from multiprocessing import cpu_count
import pandas as pd
from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired

def add_time(st,td):
    return datetime.strftime(datetime.strptime(st, '%Y-%m-%dT%H:%M:%S.%f') + timedelta(**td),'%Y-%m-%dT%H:%M:%S.%f')[:-3]
def duration_calc(process_name,process_start_time):
    complete_time = datetime.now()
    t_delta = complete_time-process_start_time
    d = divmod(t_delta.seconds,86400)  # days
    h = divmod(d[1],3600)  # hours
    m = divmod(h[1],60)  # minutes
    s = m[1]  # seconds
    #logger.debug('%s: Process Start, %s Process Complete, %s Process Duration, %s seconds' %(process_name,process_start_time,complete_time,str(t_delta)))
    print('%s: Process Start, %s Process Complete, %s Process Duration, %s hours %s minutes' %(process_name,process_start_time,complete_time,str(h),str(m)))
    #logger.info('%s: %d days, %d hours, %d minutes, %d seconds' % (process_name,d[0],h[0],m[0],s))

#splitting a dataframe into sections and
#creating the 'ranges'(variables) to be passed to the cpu-process
def cpu_args(ip_basedf,lm_day):
    ct=0
    ranges=[]
    for g, df in ip_basedf.groupby(np.arange(len(ip_basedf)) // 500):
        ct+=1
        fname=ip_dir+'ipv6_'+day_id+'-'+str(ct)+'.tsv'
        df_new = df
        df_new=df_new.reset_index(drop=True)
        ranges.append({'source':'CIMA',
        'section':ct,
        'df_500':df_new,
        'filename':str(fname),
        'lm_date':lm_day})
    return ranges

#kwargs - keyword arguments needed
def cpu_process(kwargs):
    # I think it's a tuple so?
    # only the first item needed to feed the kwargs list
    kwargs=kwargs[0]
    legalmart_start=datetime.now()
    df_part=kwargs['df_500']
    outfile=kwargs['filename']
    lm_day = kwargs['lm_date']
    lm_query_count=0
    with open(outfile, 'w', newline='\n') as f:
        print('Pooled df_part size: '+str(len(df_part)))
        #place some sort of function and then a write statement
        #a=a**2
        #due to very large files it was easier to write out the
        # data rather than keep it in memory
        if not lm_query_list:
            f.write('\t'.join([str(acct),ip,'False'])+'\n')
        else:
            acct_flag='False'
            #need to check size of lm_query_list
            # may need to do an 'IN' statement
            if (str(acct) == str(lm_query_list[0][0])):
                acct_flag='True'
            #print('\t'.join([csvrow[0],csvrow[1],acct_flag]))
            f.write('\t'.join([str(acct),ip,acct_flag])+'\n')

duration_calc('legalmart-'+str(kwargs['section']),legalmart_start)

#data read takes in headers
df = pd.read_csv(ip_acct_file[0],sep='\t')
#df = df.head(n=4000)
num_records = len(df)
print(num_records)

if cpu_count() < 10:
    cpus = cpu_count() - 1
else:
    cpus = cpu_count() - 5

#cpus = 5
process_pool_start=datetime.now()

with ProcessPool(max_workers=cpus) as pool:
    #increasing timeout feature
    #cpu_process is the function
    #cpu_args creates the arguments that are fed into cpu_process
    future = pool.map(cpu_process, product(cpu_args(df,lm_day_id)),timeout=1500)
    #future = pool.map(splunk_create_query, product(splunk_query_args(program_start_date,data_dir,file_prefix)), timeout=720)
    iterator = future.result()
    #index = 0

    while True:
        try:
            result = next(iterator)
        except StopIteration:
            break
        except TimeoutError as error:
            print("Process TimeoutError: function took longer than %d seconds" % error.args[1])
            #print(program_start_date,data_dir,file_prefix)
        except ProcessExpired as error:
            print("ProcessExpired: %s. Exit code: %d" % (error, error.exitcode))
        except Exception as error:
            print("function raised %s" % error)
            print(error.traceback)  # Python's traceback of remote process

duration_calc('Process Pool Completed: ',process_pool_start)


-- Russell Bigley
Data Scientist
Cell: (303) 916-8899
Russell_Bigley@comcast.com
