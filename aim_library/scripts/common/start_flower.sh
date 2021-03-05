#!/bin/bash

celery flower --broker=redis://:$REDIS_PASSWORD@$REDIS_CELERY_HOST:$REDIS_PORT/$REDIS_CELERY_BROKER_DB