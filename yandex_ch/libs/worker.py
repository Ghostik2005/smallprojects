# coding: utf-8

import os
import jwt
import sys
import time
import json
import glob
import traceback
import configparser
from dataclasses import dataclass

try:
    from libs.ms71lib import _client as jsonrpclib
except ImportError:
    import ms71lib._client as jsonrpclib

try:
    log = sys.log
except:
    log = None

class ApplicationException(Exception):
    pass

@dataclass
class REFS:
    url_api:str = 'https://mdb.api.cloud.yandex.net:443'
    url_manager:str = "https://resource-manager.api.cloud.yandex.net:443"
    url_iam:str = 'https://iam.api.cloud.yandex.net/iam/v1/tokens'
    url_iam_token:str = 'https://iam.api.cloud.yandex.net:443'

    def __init__(self, key_path:str=None):
        self.pem_file = key_path or '/ms71/saas/yandex_ch/private.pem'
        if not os.path.exists(self.pem_file):
            raise ApplicationException("InitError: .pem file absent")
        try:
            config = configparser.ConfigParser()
            ini_dir = os.path.dirname(self.pem_file)
            config.read(os.path.join(ini_dir, 'yandex_ch.ini'), encoding='UTF-8')
            init = config['init']
            self.service_account_id = init['service_account_id']
            self.clusterId = init['clusterId']
            self.key_id = init['key_id']
            self.folderId = init['folderId']
            try:
                wd = init['work_dir']
            except:
                wd = None
            self.work_dir = wd or '/ms71/dict'
        except:
            raise ApplicationException('InitError: .ini file error')


class YAPI:

    def __init__(self, *args, **kwargs):
        argvs = []
        name = __file__.split('/')
        key_path = None
        for i in name:
            if '.zip' in i:
                name = i
                break
        if isinstance(name, list):
            name = None
        for i, f in enumerate(sys.argv):
            if name and f == name:
                argvs = sys.argv[i+1:]
        for arg in argvs:
            try:
                k, w = arg.split('=')
                if k == 'key_path':
                    key_path = w
            except:
                pass
        
        self.refs = REFS(key_path)
        self.expiresAt = 0
        self.auth = []
        self.tasks = []
        try:
            self._gen_tasks()
        except:
            raise ApplicationException(f"GenTasks ERROR: {traceback.format_exc()}")

    def _print(self, msg, *args, **kwargs):
        if log:
            log(msg)
        else:
            print(msg, *args, **kwargs)

    def _getAuth(self):
        with open(self.refs.pem_file, 'r') as private:
            private_key = private.read() # Чтение закрытого ключа из файла.
        now = int(time.time())
        # формируем объект для авторизации
        payload = {
            'aud': self.refs.url_iam,
            'iss': self.refs.service_account_id,
            'iat': now,
            'exp': now + 3600
        }

        # Формирование JWT.
        encoded_token = jwt.encode(
            payload,
            private_key,
            algorithm='PS256',
            headers={'kid': self.refs.key_id}
        ).decode()
        data = json.dumps({'jwt': encoded_token}).encode() 
        request = self._NewRequest(self.refs.url_iam_token)
        answer = request(data, '/iam/v1/tokens')
        request.close()
        iamToken = answer.get('iamToken')
        self.expiresAt = time.mktime(time.strptime(answer.get('expiresAt'),'%Y-%m-%dT%H:%M:%S.%fZ'))
        self.auth = [
            ('Authorization', 'Bearer %s' % iamToken),
        ]

    def _NewRequest(self, url, auth=None, timeout=None):
        rpc = jsonrpclib.ServerProxy(url, verbose=False, api_key="")
        if auth is None:
            request = rpc("request")
        else:
            self._check_expire()
            if len(auth) < 1:
                auth = self.auth
            _request = rpc("request")
            request = lambda data, handler=None: _request(data, handler=handler, headers=auth)
        if timeout:
            transport = rpc("transport")
            transport._timeout = timeout
        request.close = rpc('close')
        return request

    def _check_expire(self):
        # выполняем всегда перед запросами. если токены просрочены - генерим новые
        now = time.time()
        if (isinstance(self.auth, list) and len(self.auth) < 1) or self.expiresAt - now > 600:
            self._getAuth()

    
    def _get_cloud_data(self):
        # продакшн
        request = self._NewRequest(self.refs.url_api, self.auth)
        ya_cloud_data = request('', f'/managed-clickhouse/v1/clusters?folderId={self.refs.folderId}') #описание кластера, откуда мы будем брать имена словарей
        request.close()
        return ya_cloud_data


    def _parse_cloud_data(self):
        cloud_data = self._get_cloud_data()
        try:
            dicts = cloud_data.get('clusters', {})[0].get('config', {}).get('clickhouse', {}).get('config', {}).get('effectiveConfig', {}).get('dictionaries')
        except (AttributeError, IndexError):
            dicts = []
        return [d.get('name') for d in dicts] #список имен словарей на клике

    def _get_json_files(self):
        path = os.path.join(self.refs.work_dir, '*.json')
        files = glob.glob(path)
        jsons = []
        for f in files:
            with open(f, 'r') as _f:
                data = _f.read()
                jsons.append(json.loads(data))
        return jsons

    def _gen_tasks(self):
        j_datas = self._get_json_files() # данные из json-файлов
        c_names = self._parse_cloud_data() # имена словарей на сервере
        j_names = [n.get('externalDictionary').get('name') for n in j_datas] # имена словарей из json файлов
        for c_name in c_names:
            # если имени нет в файлах - добавляем задание на удаление словаря из кластера
            if not c_name in j_names:
                self.tasks.append({
                    'method': 'deleteExternalDictionary',
                    'payload': {
                        "externalDictionaryName": c_name
                    }
                })
        for i, j_name in enumerate(j_names):
            # еслли нет в именах в кластере - добавляем задание на добавление словаря
            if not j_name in c_names:
                self.tasks.append({
                    'method': 'createExternalDictionary',
                    'payload': j_datas[i]
                })

    def process(self):
        try:
            while len(self.tasks) > 0:
                task = self.tasks.pop()
                request = self._NewRequest(self.refs.url_api, self.auth)
                answer = request(json.dumps(task['payload']).encode(), '/managed-clickhouse/v1/clusters/{clusterId}:{method}'.format(clusterId=self.refs.clusterId, method=task['method']))
                self._print(answer, flush=True)
                request.close()
        except:
            raise ApplicationException(f"ProcessTasks ERROR: {traceback.format_exc()}")


def start(*args, **kwargs):
    
    y = YAPI(*args, **kwargs)
    y.process()

if __name__ == '__main__':
    start()


