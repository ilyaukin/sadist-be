from argparse import ArgumentParser

from scheduler.task_queue import run_queue


argparser = ArgumentParser(description='Run task queue')
argparser.add_argument('--workers', type=int, default=1)
argparser.add_argument('--worker-poll-interval', type=int, default=5)
argparser.add_argument('--monitor-poll-interval', type=int, default=30)
args = argparser.parse_args()

run_queue(
    worker_count=args.workers,
    worker_poll_interval=args.worker_poll_interval,
    monitor_poll_interval=args.monitor_poll_interval,
)
