# coding=utf-8
import time;
from multiprocessing.managers import BaseManager
import os, sys
import extends;
import etl
import json
authkey= "etlpy".encode('utf-8')
timeout=1;
rpc_port=28998




from flask import request,jsonify
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


from flask import Flask

finished_job=[]
dispatched_job=[]
app = Flask(__name__)

class Master:
    def __init__(self):
        # 派发出去的作业队列
        self.dispatched_job_queue = Queue()
        # 完成的作业队列
        self.finished_job_queue = Queue()


    def get_dispatched_job_queue(self):
        return self.dispatched_job_queue

    def get_finished_job_queue(self):
        return self.finished_job_queue

    def server_init(self, port=None):
        if port is None:
            port = rpc_port;
        # 把派发作业队列和完成作业队列注册到网络上
        BaseManager.register('get_dispatched_job_queue', callable=self.get_dispatched_job_queue)
        BaseManager.register('get_finished_job_queue', callable=self.get_finished_job_queue)
        print('current port is %d' % port)
        # 监听端口和启动服务

        manager.start()
        print('server started');
        if extends.is_ipynb:
            print('exec in ipython notebook')

    def start_server(self, port=None):
        self.server_init(port)
        app.run(host='0.0.0.0', port=60007, debug=True, use_reloader=False)

    def start_project(self,project, job_name,take=100000,skip=0,port=None, monitor_connector_name=None,table_name=None):
        self.server_init(port)
        self.max_process = 10;
        self.monitor = None;
        if monitor_connector_name is not None:
            monitor = self.project.connectors.get(monitor_connector_name, None);
            monitor.server_init();
            if monitor is not None:
                if table_name is None:
                    table_name = job_name + '_monitor';
                self.monitor = monitor._db[table_name];
        module= self.project.modules[job_name];
        proj= etl.convert_dict(project);
        mapper, reducer, parallel = etl.parallel_map(module.tools)
        if parallel is None:
            print 'this script do not support pl...'
            return
        dispatched_count = 10;
        dispatched_jobs = self.manager.get_dispatched_job_queue()
        finished_jobs = self.manager.get_finished_job_queue()
        job_id = 0
        try:
            while True:
                while True:
                    i = 0;
                    for task in  self._pl_generator(take,skip):
                        i += 1;
                        job_id = job_id + 1
                        job = {'proj':proj,'name':job_name,'tasks':task,'id':job_id}
                        if not extends.is_ipynb:
                            pass;
                            # print('dispatch job: {id}, count : {count} '.script (id=job.id,count=count_per_group))
                        dispatched_jobs.put(job)
                        if i % dispatched_count == 0:
                            while not dispatched_jobs.empty():
                                job = finished_jobs.get(60)
                                if self.monitor is not None:
                                    monitor = self.monitor;
                                    for task in job.tasks:
                                        task_dict = task.__dict__;
                                        item = monitor.update({'m_id': task.m_id, 'id': task.id}, task_dict,
                                                              upsert=True);
                                if not extends.is_ipynb:
                                    pass;

                                    # print('finish job: {id}, count : {count} '.script(id=job.id, count=job.count))

                if not extends.is_ipynb:
                    key = input('press any key to repeat,c to cancel')
                    if key == 'c':
                        break;
            manager.shutdown()
        except Exception as e:
            import traceback
            traceback.print_exc()
            print('manager has shutdown')
            manager.shutdown();

@app.route('/task/query/<method>', methods=['GET'])
def query_task(method):
    job_queue = manager.get_finished_job_queue()
    while not job_queue.empty():
        job = job_queue.get(60)
        finished_job.append(job)
    result = {"remain": manager.get_dispatched_job_queue().qsize()}
    if method=='finished':
        result[method]= finished_job;
    elif method=='dispatched':
        result[method]=dispatched_job
    elif method=='clean':
        for r in dispatched_job:
            dispatched_job.remove(r)
    result = jsonify(**result)
    return result


@app.route('/task/insert', methods=['POST'])
def insert_task():
    js = request.json;  # have no the json data ?

    dispatched_job_queue = manager.get_dispatched_job_queue()

    result=  {"status":"success","remain": manager.get_finished_job_queue().qsize()};
    for r in dispatched_job:
        if r['id']==js['id'] and r['name']==js['name']:
            result['status']='repeat,ignore'
            return jsonify(**result)

    dispatched_job_queue.put(js)
    import copy
    js2=copy.copy(js)
    del js2['proj']

    dispatched_job.append(js2)
    result = jsonify(**result);
    return result

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
                print('task finished, delay 20s')
                time.sleep(20);
                continue;

            job = dispatched_jobs.get(timeout=timeout)
            project,name,tasks,job_id=job['proj'],job['name'],job['tasks'],job['id']
            print('Run job: %s ' % job_id)
            proj= etl.Project();
            proj.load_dict(project);
            module= proj.env[name];
            task_results=[]
            total_count=0;
            try:
                if not isinstance(tasks,list):
                    tasks=[tasks];
                mapper,reducer,tool= etl.parallel_map(module.tools);
                for i in range(len(tasks)):
                    task=tasks[i];
                    task_result=Task(job_id,i,task);
                    count=0;
                    try:

                        generator= etl.ex_generate(reducer, generator=[task], execute= execute)
                        for r in generator:
                            #print(r.keys())
                            count+=1;
                            total_count+=1;
                        task_result.time=time.time()
                        task_result.count=count;
                        task_result.message='success';
                    except Exception as e:
                        task_result.message=str(e);
                    task_results.append(task_result.__dict__)
            except Exception as e:
                import traceback
                traceback.print_exc()
            print('finish job,id %s, count %s'%(job_id,total_count))
            job_result= {'name':name,'result':task_results,'job_id': job_id,'total':total_count};
            finished_jobs.put(job_result)


if __name__ == '__main__':

    parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/src'
    sys.path.insert(0, parentdir)

    manager = BaseManager(address=('0.0.0.0', rpc_port), authkey=authkey)
    ip= '127.0.0.1' #'10.101.167.107'
    #ip= '10.101.167.107'
    port=rpc_port;
    mode='client'
    argv=sys.argv;
    if len(argv) > 1:
        mode = argv[1];
    if mode == 'server':
        if len(argv)>2:
            port=argv[2]
        master = Master();
        master.start_server(None);
    else:
        if len(argv) > 2:
            ip=argv[2]

        if len(argv)>3:
            port=int(argv[3]);
        slave= Slave();
        slave.start(True,ip,port);


