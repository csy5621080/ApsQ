import redis
import pickle


class QueueBase(object):

    """
        Redis 队列基类
    """
    def __init__(self, **base_conn):
        self._queue_base = redis.StrictRedis(**base_conn, db=1)


class Queue(QueueBase):

    """
        Redis 队列
    """
    def __init__(self, name):
        self.__key = f'cus_queue:{name}'
        self.__running_obj_key = f'cus_queue:{name}:runner'
        super().__init__()

    def push(self, *value):
        """
            向队尾新增元素
        :param value: list or tuple
        :return:
        """
        self._queue_base.lpush(self.__key, *[pickle.dumps(v, pickle.HIGHEST_PROTOCOL) for v in value])

    def jump(self, *value):
        """
            向队首新增元素
        :param value: list or tuple
        :return:
        """
        self._queue_base.rpush(self.__key, *[pickle.dumps(v, pickle.HIGHEST_PROTOCOL) for v in value])

    def wait(self):
        """
            阻塞式获取元素.
            当队列为空时， 阻塞.
        :return:
        """
        obj = self._queue_base.brpop(self.__key, None)
        return pickle.loads(obj[1])

    @property
    def runner(self):
        obj = self._queue_base.get(self.__running_obj_key)
        if obj:
            return pickle.loads(obj)
        else:
            return None

    @runner.setter
    def runner(self, obj):
        self._queue_base.set(self.__running_obj_key, pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))

    @runner.deleter
    def runner(self):
        self._queue_base.delete(self.__running_obj_key)

    def clear(self):
        task_list = []
        while True:
            obj = self._queue_base.rpop(self.__key)
            if obj:
                task_list.append(pickle.loads(obj))
            else:
                break
        return task_list

    def get_all(self):
        task_list = self.clear()
        self.push(*task_list)
        return task_list


class JobState(QueueBase):

    """
        任务状态.
        通过 job_id 精确命中需要的任务， 以操作具体job的状态
    """

    END = 1         # 结束
    ERROR = 2       # 出错
    RUNNING = 3     # 正在运行

    def __init__(self, job_id):
        self.__key = f'cus_job_state:{job_id}'
        self.__pause_sign_key = f'cus_job_state:{job_id}:is_paused'
        super().__init__()

    @property
    def state(self):
        status_code = self._queue_base.brpop(self.__key, None)
        return status_code[1].decode()

    @state.setter
    def state(self, status_code):
        self._queue_base.lpush(self.__key, str(status_code))

    """
        将暂停的主动动作与其他状态做区分.
    """
    @property
    def paused(self):
        if self._queue_base.llen(self.__pause_sign_key) > 0:
            return 1
        else:
            return 0

    @paused.setter
    def paused(self, code):
        self._queue_base.lpush(self.__pause_sign_key, code)

    def destroy(self):
        for i in range(self._queue_base.llen(self.__pause_sign_key)):
            self._queue_base.rpop(self.__pause_sign_key)
        for i in range(self._queue_base.llen(self.__key)):
            self._queue_base.rpop(self.__key)