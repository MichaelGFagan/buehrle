from prefect import flow, task
import anyio
import time

@task
def process_data(data):
    time.sleep(5)  # Simulate processing
    return f"Processed {data}"

@flow
def subflow(data):
    result = process_data(data)
    return result

@flow
async def main_flow():
    start = time.time()
    
    # Create tasks to run each subflow in a thread
    subflow_tasks = []
    for data in ["data1", "data2", "data3"]:
        # anyio.to_thread.run_sync preserves task context
        task = anyio.to_thread.run_sync(subflow, data)
        subflow_tasks.append(task)
    
    # Run all in parallel and collect results
    results = await asyncio.gather(*subflow_tasks)
    
    end = time.time()
    print(f"Time taken: {end - start:.2f} seconds")  # Should be ~1 second
    
    return results

if __name__ == "__main__":
    import asyncio
    result = asyncio.run(main_flow())
    print(result)

# import httpx
# from datetime import timedelta
# from prefect import flow, task
# from prefect.tasks import task_input_hash
# from typing import Optional


# @task(cache_key_fn=task_input_hash, refresh_cache=True)
# def get_url(url: str, params: Optional[dict[str, any]] = None):
#     response = httpx.get(url, params=params)
#     response.raise_for_status()
#     return response.json()


# def get_open_issues(repo_name: str, open_issues_count: int, per_page: int = 100):
#     issues = []
#     pages = range(1, -(open_issues_count // -per_page) + 1)
#     for page in pages:
#         issues.append(
#             get_url.submit(
#                 f"https://api.github.com/repos/{repo_name}/issues",
#                 params={"page": page, "per_page": per_page, "state": "open"},
#             )
#         )
#     return [i for p in issues for i in p.result()]


# @flow(retries=3, retry_delay_seconds=5, log_prints=True)
# def get_repo_info(repo_name: str = "PrefectHQ/prefect"):
#     repo_stats = get_url(f"https://api.github.com/repos/{repo_name}")
#     issues = get_open_issues(repo_name, repo_stats["open_issues_count"])
#     issues_per_user = len(issues) / len(set([i["user"]["id"] for i in issues]))
#     print(f"{repo_name} repository statistics 🤓:")
#     print(f"Stars 🌠 : {repo_stats['stargazers_count']}")
#     print(f"Forks 🍴 : {repo_stats['forks_count']}")
#     print(f"Average open issues per user 💌 : {issues_per_user:.2f}")


# if __name__ == "__main__":
#     get_repo_info()

# from prefect import task, flow

# test = [1, 2, 3]

# @task
# def test_task(number):
#     return number + 1

# @flow
# def test_flow():
#     results = test_task.map(test)
#     return results

# results = test_flow()
# print(results.result())

# from prefect import flow, task
# from prefect.task_runners import ThreadPoolTaskRunner
# from concurrent.futures import ThreadPoolExecutor
# from time import sleep

# @task
# def test_task(number):
#     sleep(5)
#     return number + 1

# @task
# def test_task_two(number):
#     sleep(5)
#     return number * 2

# @flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
# def test_flow():
#     results = test_task.map(list(range(1, 10)))
#     return results

# @flow(task_runner=ThreadPoolTaskRunner(max_workers=5))
# def test_flow_two():
#     results = test_task_two.map(list(range(1, 10)))
#     return results

# @flow
# def nested_flows():
#     with ThreadPoolExecutor() as executor:
#         flows = [test_flow(), test_flow_two()]
#         for flow in flows:
#             flow
    

# if __name__ == '__main__':
#     nested_flows()