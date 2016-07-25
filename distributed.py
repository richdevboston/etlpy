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

    def __init__(self,project,jobname):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()
        self.project= project;
        self.jobname=jobname;
        self.maxprocess= 10;

    def get_dispatched_job_queue(self):
        return self.dispatched_job_queue

    def get_finished_job_queue(self):
        return self.finished_job_queue

    def start(self,skip=0,count=100000):
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue', callable=self.get_dispatched_job_queue)
        BaseManager.register('get_finished_job_queue', callable=self.get_finished_job_queue)

        # 监听端口和启动服务
        manager = BaseManager(address=('0.0.0.0', rpc_port), authkey=authkey)
        manager.start()

        # 使用上面注册的方法获取队列
        dispatched_jobs = manager.get_dispatched_job_queue()
        finished_jobs = manager.get_finished_job_queue()

        job_id = 0
        module= self.project.modules[self.jobname];

        proj=json.loads(self.project.dumps_json() )
        while True:
            for task in etl.generate(etl.parallel_map(module.tools)[0]):
                try:
                    job_id = job_id + 1
                    if job_id<skip:
                        continue
                    if job_id>count:
                        break;
                    job = ETLJob(proj, self.jobname, task, job_id);
                    if job_id%10==0 and job_id >0:
                        print('Dispatch job: %s - %s' % (str(job.id-10),str(job.id)))
                    dispatched_jobs.put(job)
                except Exception as e:
                    print(e);
                    continue;
            print('all task dispatched...')
            while not dispatched_jobs.empty():
                job = finished_jobs.get(60)
                print('Finished Job: %s, Count: %s' % (job.id, job.count))

            key=input('press any key to repeat,c to cancel')
            if key=='c':
                manager.shutdown()
                break

        #manager.shutdown()






class Slave:

    def __init__(self):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()
    def start(self,execute= True,serverip='127.0.0.1',port=8888):
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
                generator= etl.generate(etl.parallel_map(module.tools)[1],generator=config,execute= execute)
                for r in generator:
                    #print(r.keys())
                    count+=1;
            except Exception as e:
                print(e)
            print('finish job,id %s, count %s'%(job.id,count))
            resultjob= JobResult(job.jobname,count,job.id)

            finished_jobs.put(resultjob)


if __name__ == '__main__':
    ip='10.101.167.107'
    port=rpc_port;
    argv=sys.argv;
    if len(argv)>1:
        ip=argv[1];
    if len(argv)>2:
        port=int(argv[2]);
    slave= Slave();
    slave.start(True,ip,port);


