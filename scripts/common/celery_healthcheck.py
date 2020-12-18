from tasks.worker import celery
from celery.result import AsyncResult
from os import environ

if __name__ == '__main__':
    with open(f'{environ["HOME"]}/task_ids', 'r') as ids_file:
        ids = ids_file.readline().split(',')
        for id in ids:
            ar = AsyncResult(id)
            if ar.status == 'FAILURE':
                exit(1)
