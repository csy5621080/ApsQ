from listener import listener_handler, QueueListener
from Qbase import Queue, JobState
import time


class MyTask(object):

    def __init__(self, id, name, **kwargs):
        self.id = id
        self.name = name
        ...
        # 可以自行配置自己需要的参数


def my_callback_func(task, job_id):
    my_job_state = JobState(job_id)
    """do something"""
    i = 0
    while i < 100:
        if my_job_state.paused:             # 判断是否被暂停
            break
        time.sleep(1)
        print(f'ID: {task.id}, name: {task.name}, loop_num: {i}')
        i += 1


@listener_handler.task
class MyQueueListener(QueueListener):
    queue_name = 'my_queue'
    __queue__ = Queue(queue_name)
    __ended_queue__ = Queue(f'{queue_name}:ended')
    __run_func__ = my_callback_func


# 获取正在执行的任务
q = Queue('my_queue')
running_task: MyTask = q.runner  # MyTask对象
# 获取job 状态, 如 running_task 的id为唯一标识
job_id = running_task.id
job_state = JobState(job_id)
# 暂停任务
job_state.paused = 1
# 队列新加任务
tasks = [MyTask(1, '1'), MyTask(2, '2')]
q.push(*tasks)
# 队列插队
jump_task = MyTask(3, '3')
q.jump(jump_task)
