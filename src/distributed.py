# coding=utf-8
import  sys;
import time;
from multiprocessing.managers import BaseManager

import extends;
import etl

authkey= "etlpy".encode('utf-8')
timeout=1;
rpc_port=28998

if extends.PY2:
    from Queue import Queue
else:
    from queue import Queue
class ETLJob:
    def __init__(self, project, job_name, config, id):
        self.project= project;
        self.job_name=job_name;
        self.config=config;
        self.id= id;

class Task:
    def __init__(self,m_id,id,task):
        self.m_id=m_id
        self.id=id
        self.count=0;
        self.message=''
        self.task=task


class JobResult:
    def __init__(self,name,tasks,id,total_count):
        self.name=name;
        self.tasks=tasks;
        self.id=id;
        self.total_count=total_count;


class Master:
    def __init__(self, project, job_name,monitor_connector_name=None,table_name=None):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()
        self.project= project;
        self.job_name=job_name;
        self.max_process= 10;
        self.monitor=None;
        if monitor_connector_name is not None:
            monitor=self.project.connectors.get(monitor_connector_name,None);
            monitor.init();
            if monitor is not None:
                if table_name is None:
                    table_name=job_name+'_monitor';
                self.monitor=monitor._db[table_name];

    def get_dispatched_job_queue(self):
        return self.dispatched_job_queue

    def get_finished_job_queue(self):
        return self.finished_job_queue

    def start(self,take=100000,skip=0,port=None):
        if port is None:
            port=rpc_port;
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue', callable=self.get_dispatched_job_queue)
        BaseManager.register('get_finished_job_queue', callable=self.get_finished_job_queue)
        print('current port is %d'%port)
        # 监听端口和启动服务
        manager = BaseManager(address=('0.0.0.0', port), authkey=authkey)

        self.manager=manager;
        manager.start()
        print('server started');
        if extends.is_ipynb:
            print('exec in ipython notebook')
        dispatched_count=10;
        # 使用上面注册的方法获取队列
        dispatched_jobs = manager.get_dispatched_job_queue()
        finished_jobs = manager.get_finished_job_queue()
        job_id = 0
        module= self.project.modules[self.job_name];
        proj= etl.convert_dict(self.project);

        mapper, reducer, parallel = etl.parallel_map(module.tools)

        count_per_group = parallel.count_per_thread if parallel is not None else 1;
        task_generator= extends.group_by_mount(etl.generate(mapper), count_per_group, take, skip);
        task_generator = extends.progress_indicator(task_generator, 'Task Dispatcher', etl.count(mapper))
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
                            #print('dispatch job: {id}, count : {count} '.script (id=job.id,count=count_per_group))
                        dispatched_jobs.put(job)
                        if i%dispatched_count==0:
                            while not dispatched_jobs.empty():
                                job = finished_jobs.get(60)
                                if self.monitor is not None:
                                    monitor= self.monitor;
                                    for task in job.tasks:
                                        task_dict=task.__dict__;
                                        item=monitor.update({'m_id':task.m_id,'id':task.id},task_dict,upsert=True);
                                if not extends.is_ipynb:
                                    pass;

                            #print('finish job: {id}, count : {count} '.script(id=job.id, count=job.count))

                if not  extends.is_ipynb:
                    key=input('press any key to repeat,c to cancel')
                    if key=='c':
                        break;
            manager.shutdown()
        except Exception as e:
            import traceback
            traceback.print_exc()
            print('manager has shutdown')
            manager.shutdown();





class Slave:

    def __init__(self):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()
    def start(self, execute= True, server_ip='127.0.0.1', port=rpc_port):
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue')
        BaseManager.register('get_finished_job_queue')

        server = server_ip;
        print('Connect to server %s...' % server)
        manager = BaseManager(address=(server, port), authkey=authkey)
        manager.connect()
        # 使用上面注册的方法获取队列
        dispatched_jobs = manager.get_dispatched_job_queue()
        finished_jobs = manager.get_finished_job_queue()

        # 运行作业并返回结果，这里只是模拟作业运行，所以返回的是接收到的作业
        while True:
            if dispatched_jobs.empty():
                print('task finished, delay 5s')
                time.sleep(5);
                continue;

            job = dispatched_jobs.get(timeout=timeout)
            print('Run job: %s ' % job.id)
            project=job.project;
            proj= etl.Project();
            proj.load_dict(project);
            module= proj.modules[job.job_name];
            task_results=[]
            total_count=0;
            try:
                tasks= job.config;
                if not isinstance(tasks,list):
                    tasks=[tasks];
                mapper,reducer,tool= etl.parallel_map(module.tools);
                for i in range(len(tasks)):
                    task=tasks[i];
                    task_result=Task(job.id,i,task);
                    count=0;
                    try:
                        generator= etl.generate(reducer, generator=[task], execute= execute)
                        for r in generator:
                            #print(r.keys())
                            count+=1;
                            total_count+=1;
                        task_result.count=count;
                        task_result.message='success';

                    except Exception as e:
                        task_result.message=e;
                    task_results.append(task_result)
            except Exception as e:
                import traceback
                traceback.print_exc()
            print('finish job,id %s, count %s'%(job.id,total_count))
            job_result= JobResult(job.job_name,task_results,job.id,total_count)
            finished_jobs.put(job_result)

if __name__ == '__main__':
    import os, sys
    parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/src'
    sys.path.insert(0, parentdir)
    ip= '127.0.0.1' #'10.101.167.107'
    port=rpc_port;
    argv=sys.argv;
    if len(argv)>1:
        ip=argv[1];
    if len(argv)>2:
        port=int(argv[2]);
    slave= Slave();
    slave.start(True,ip,port);


