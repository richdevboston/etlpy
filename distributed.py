# coding=utf-8
import  sys;
from queue import Queue
from multiprocessing.managers import BaseManager
import etl;
import json
import extends;
import time;
authkey= "etlpy".encode('utf-8')
timeout=1;
rpc_port=8998
from progressive import *

class ETLJob:
    def __init__(self,project,jobname,config,id):
        self.project= project;
        self.jobname=jobname;
        self.config=config;
        self.id= id;

class JobResult:
    def __init__(self,name,count,id):
        self.name=name;
        self.count=count;
        self.id=id;

class Master:

    def __init__(self, project, job_name):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()
        self.project= project;
        self.job_name=job_name;
        self.max_process= 10;

    def get_dispatched_job_queue(self):
        return self.dispatched_job_queue

    def get_finished_job_queue(self):
        return self.finished_job_queue

    def start(self,take=100000,skip=0,port=rpc_port):
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue', callable=self.get_dispatched_job_queue)
        BaseManager.register('get_finished_job_queue', callable=self.get_finished_job_queue)
        print('current port is %d'%rpc_port)
        # 监听端口和启动服务
        manager = BaseManager(address=('0.0.0.0', rpc_port), authkey=authkey)
        manager.start()
        dispatched_count=10;
        # 使用上面注册的方法获取队列
        dispatched_jobs = manager.get_dispatched_job_queue()
        finished_jobs = manager.get_finished_job_queue()

        job_id = 0
        module= self.project.modules[self.job_name];
        proj= etl.convert_dict(self.project);
        mapper, reducer, tolist = etl.parallel_map(module.tools)
        count_per_group = tolist.MountPerThread;
        task_generator=extends.group_by_mount(etl.generate(mapper),count_per_group, take,skip);
        from ipy_progressbar import ProgressBar
        task_generator = ProgressBar(task_generator, title='Task Dispatcher')
        task_generator.max=100;
        task_customer = ProgressBar(dispatched_count,title='Task Customer  ')
        task_customer.start()
        try:
            while True:
                while True:
                    i=0;
                    for task in task_generator:
                        i+=1;
                        job_id = job_id + 1
                        job = ETLJob(proj, self.job_name, task, job_id);
                        if not extends.is_ipynb:
                            pass;
                            #print('dispatch job: {id}, count : {count} '.format (id=job.id,count=count_per_group))
                        dispatched_jobs.put(job)
                        if i%dispatched_count==0:
                            task_customer.start()
                            while not dispatched_jobs.empty():
                                job = finished_jobs.get(60)
                                task_customer.advance()
                                if not extends.is_ipynb:
                                    pass;
                                    #print('finish job: {id}, count : {count} '.format(id=job.id, count=job.count))

                key=input('press any key to repeat,c to cancel')
                if key=='c':
                    break;
                    manager.shutdown()
        except Exception as e:
            print('manager has shutdown')
            manager.shutdown();




class Slave:

    def __init__(self):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()
    def start(self,execute= True,serverip='127.0.0.1',port=rpc_port):
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue')
        BaseManager.register('get_finished_job_queue')

        server = serverip;
        print('Connect to server %s...' % server)
        manager = BaseManager(address=(server, port), authkey=authkey)
        manager.connect()
        # 使用上面注册的方法获取队列
        dispatched_jobs = manager.get_dispatched_job_queue()
        finished_jobs = manager.get_finished_job_queue()

        # 运行作业并返回结果，这里只是模拟作业运行，所以返回的是接收到的作业
        while True:
            if dispatched_jobs.empty():
                time.sleep(1)
                print('queue is empty,wait 1 sec...')
                continue;

            job = dispatched_jobs.get(timeout=timeout)
            print('Run job: %s ' % job.id)
            project=job.project;
            proj= etl.Project();
            proj.load_dict(project);
            module= proj.modules[job.jobname];
            count=0
            try:
                config= job.config;
                if not isinstance(config,list):
                    config=[config];
                mapper,reducer,tool= etl.parallel_map(module.tools);
                generator= etl.generate( reducer,generator=config,execute= execute)
                for r in generator:
                    #print(r.keys())
                    count+=1;
            except Exception as e:
                print(e)
            print('finish job,id %s, count %s'%(job.id,count))
            job_result= JobResult(job.jobname,count,job.id)
            finished_jobs.put(job_result)


if __name__ == '__main__':
    ip= '127.0.0.1' #'10.101.167.107'
    port=rpc_port;
    argv=sys.argv;
    if len(argv)>1:
        ip=argv[1];
    if len(argv)>2:
        port=int(argv[2]);
    slave= Slave();
    slave.start(True,ip,port);


