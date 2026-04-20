from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
import loaders.fangraphs as fg
import loaders.baseball_reference_war as bbwar
import loaders.chadwick_register as chadwick

@flow
def main():
    logger = get_run_logger()
    tasks = [
        # bbwar.bbref_war.submit(logger),
        fg.fangraphs.submit(1871, 2024, logger),
        # chadwick.chadwick_register.submit(logger)
        ]
    [task.result() for task in tasks]

if __name__ == '__main__':
    main()

# @flow
# async def main():
# logger = get_run_logger()
# bb_ref_task = asyncio.to_thread(bbwar.bbref_war, logger)
# fangraphs_task = asyncio.to_thread(fg.fangraphs, 2020, 2024, logger)
# await asyncio.gather(bb_ref_task, fangraphs_task)

# if __name__ == '__main__':
# import asyncio
# asyncio.run(main())