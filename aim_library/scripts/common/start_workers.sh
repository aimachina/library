#!/bin/bash

echo Listening on queues: $QUEUES,$HOSTNAME
celery worker -A tasks.worker -E            \
        -Q $QUEUES,$HOSTNAME                \
        --loglevel=WARNING                  \
        --hostname=${QUEUES}@%h-$$          \
        --prefetch-multiplier=1             \
        --autoscale=8,32