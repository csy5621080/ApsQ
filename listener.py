from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.triggers.date import DateTrigger
import threading
import atexit
from Qbase import JobState

jobstores = {
    'default': RedisJobStore(db=4)
}

scheduler = BackgroundScheduler(jobstores=jobstores)


class CUSListenerHandler(object):
    """
        所有listener的执行者.
        当前，仅有downloader一种...
    """

    def __init__(self):
        self.listeners = []

    def task(self, cls):
        """
            任务注册装饰器函数
        :param func:    listener 方法
        :return:
        """
        self.listeners.append(cls.run)

    def start(self):
        """
            所有listener统一启动
        :return:
        """
        for listener in self.listeners:
            threading.Thread(target=listener, daemon=True).start()


listener_handler = CUSListenerHandler()


class ApSchedulerSingleListener(object):
    """
        通过__call__方法动态构建回调方法.
        以精确命中要监控的Job.
    """

    def __init__(self, job_id):
        self.job_id = job_id

    def __call__(self, event):
        if str(event.job_id) == self.job_id:
            if event.exception:
                JobState(self.job_id).state = JobState.ERROR
            else:
                JobState(self.job_id).state = JobState.END


class QueueListener(object):

    __queue__ = None  # Queue(queue_name)
    __ended_queue__ = None  # Queue(f'{queue_name}:ended')
    __run_func__ = None

    @classmethod
    def run(cls):

        if cls.__queue__ is None or cls.__ended_queue__ is None:
            raise Exception('监听队列不可为空.')

        @atexit.register
        def queue_clear():
            del cls.__queue__.runner

        scheduler.start()
        while True:
            # 阻塞式获取下载的digest, 上游url和源id
            _task = cls.__queue__.wait()
            job_id = _task.digest
            # 生成当前job对应的listener方法
            job_listener_func = ApSchedulerSingleListener(job_id)
            # 获取job状态的入口
            job_state = JobState(job_id)
            # 添加job listener
            scheduler.add_listener(job_listener_func, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            # 添加job
            scheduler.add_job(id=job_id, func=cls.__run_func__, args=(_task, job_id), trigger=DateTrigger())
            # 添加job 状态为 running
            job_state.state = JobState.RUNNING
            # 标记正在下载的文件digest信息
            cls.__queue__.runner = _task

            def task_listener():
                # 阻塞式获取job状态
                status_code = int(job_state.state)
                if status_code == JobState.RUNNING:
                    # running则继续监听
                    task_listener()
                else:
                    print(f'downloader status code: {status_code}.')
                    if status_code == JobState.ERROR:
                        # todo add log
                        pass
                    else:
                        pass

            # 监听job状态
            task_listener()
            # 销毁 job state
            job_state.destroy()
            # 移除 job listener
            scheduler.remove_listener(job_listener_func)
            # 单次下载任务结束, 移除正在下载文件标记
            del cls.__queue__.runner
            # 任务结束后, 推入完成队列.
            cls.__ended_queue__.push(_task)
