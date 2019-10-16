#coding: utf-8

import time
import queue
import atexit
import psycopg2
import threading


class Connect:
    """
    Connect class
    """
    def __init__(self, parent):
        self.parent = parent
        self.con_params = parent.connection_params
        try:
            self.con = psycopg2.connect(**self.con_params)
        except:
            print('not connected', flush=True)
            self.con = None

    def kill(self):
        """
        закрываем соединение
        """
        try:
            self.con.close()
        except:
            pass
        else:
            pass
            self.con = None

    def close(self):
        """
        возвращаем соединение в пул
        """
        self.reset()
        return self.parent._close(self)

    def cursor(self):
        """
        создаем курсор
        """
        if not self.con:
            print("p-> ", self.parent.connection_queue)
            self.con = psycopg2.connect(**self.con_params)
        
        return self.con.cursor()

    def commit(self):
        """
        делаем коммит
        """
        return self.con.commit()

    def rollback(self):
        """
        делаем откат
        """
        return self.con.rollback()

    def reset(self):
        """
        сбрасываем соединение
        """
        return self.con.reset()


class ConectPool:
    """
    Pool connection class
    """
    downtime = 30 #время простоя, после которого все соединения убиваются

    def __init__(self, pool_size:int=5, connection_params:dict=None):
        self.last_request = time.time()
        self.connection_params = connection_params
        self.psize = pool_size
        self.connection_queue = queue.Queue()
        self._init_queue()
        atexit.register(self._kill_all)
        threading.Thread(target=self._check_queue, daemon=True).start()

    def _init_queue(self):
        """
        начальная иннициализация очереди
        """
        for _ in range(self.psize):
            self.connection_queue.put(self._make_connection())

    def _make_connection(self):
        """
        создаем новое подключение к базе
        """
        c = Connect(self)
        # print("connection=", c, flush=True)
        return c

    def _check_queue(self):
        """
        запускаем в цикле функцию, которая проверяет длину очереди.
        если больше psize - убиваем лишние
        """
        while True:
            dt = time.time() - self.last_request
            # print('dt:', dt, 'd:', self.downtime, 'size:', self.connection_queue.qsize(), flush=True)
            if dt > self.downtime*20 and not self.connection_queue.empty():
                #если разница между последним использованием соединния и текущим временм больше downtime то убиваем все лишние
                self._kill_all()
            if dt > self.downtime:
                #если разница между последним использованием соединния и текущим временм больше downtime то убиваем все лишние
                size = self.connection_queue.qsize()
                dq = int(size-self.psize)
                for _ in range(dq):
                    try:
                        c = self.connection_queue.get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        self.connection_queue.task_done()
                        self._kill(c)

            time.sleep(1)

    def connect(self):
        """
        берем из очереди соединение и возвращаем его
        если в очереди пусто - создаем новое соединение
        """
        self.last_request = time.time()
        # print('empty: ', str(self.connection_queue.empty()))
        # print('qsize: ', self.connection_queue.qsize())
        try:
            c = self.connection_queue.get_nowait()
        except queue.Empty:
            # print('empty', flush=True)
            c = self._make_connection()
        else:
            self.connection_queue.task_done()
        return c

    def _close(self, connection):
        """
        помещаем соединение обратно в очередь
        """
        self.last_request = time.time()
        # connection.reset()
        self.connection_queue.put(connection)


    def _kill(self, connection):
        """
        закрываем соединение
        """
        # print('kill:', connection, flush=True)
        return connection.kill()

    def _kill_all(self):
        """
        закрываем все соединения
        """
        while not self.connection_queue.empty():
            try:
                c = self.connection_queue.get_nowait()
            except queue.Empty:
                pass
            else:
                self.connection_queue.task_done()
                self._kill(c)
        # self.connection_queue = queue.Queue()

def test():
    conn = {'dbname': 'spr', 'user': 'postgres', 'host': 'localhost', 'port': 5432}
    c = ConectPool(connection_params=conn)
    con = c.connect()
    cur = con.cursor()
    cur.execute("select count(*) from spr")
    print(cur.fetchall())
    con.close()
    # c.close(con)
    time.sleep(10)
    print(c.connection_queue.qsize())
    time.sleep(1)
    con = c.connect()
    cur = con.cursor()
    print(c.connection_queue.qsize())
    cur.execute("select count(*) from spr")
    print(cur.fetchall())
    con.close()
    # c.close(con)
    print(c.connection_queue.qsize())
    while True:
        time.sleep(1)


if "__main__" == __name__:
    test()
    pass
