import json
import os
import time
from datetime import datetime

import boto3
import pytz
import requests
from boto3.dynamodb.conditions import Key

### =================================== Dynamo ============================ ###
from api_main import publish_job_status
from main import main
from return_result import push_result_orderbank


def get_job_history_table():
    dynamodb = boto3.resource('dynamodb', region_name="ap-southeast-1",
                              endpoint_url="https://dynamodb.ap-southeast-1.amazonaws.com")
    print("Getting job details from dynamodb")
    table_name = os.getenv("JOB_TABLE", None)
    if table_name is None:
        raise Exception("Table name could not be resolved from the env variable (JOB_TABLE)")
    print("using table name", table_name)
    return dynamodb.Table(table_name)


def get_job_details(instance_id):
    table = get_job_history_table()
    filtering_exp = Key("instance_id").eq(instance_id)
    response = table.query(KeyConditionExpression=filtering_exp, ConsistentRead=True)
    print("Dynamo lookup by instance id = {instance_id} result = ", response)
    items = response["Items"]
    print("items", items)
    if len(items) == 1:
        return items[0]
    return None


def update_job_progress(job_details):
    table = get_job_history_table()
    current_state = job_details["current_state"]
    response = table.update_item(
        Key={
            'instance_id': job_details["instance_id"]
        },
        UpdateExpression=f"set updated_at=:u, progress=:p, current_state=:c,{current_state}=:s",
        ExpressionAttributeValues={
            ':u': job_details["updated_at"],
            ':p': job_details["progress"],
            ':c': job_details["current_state"],
            ':s': job_details[job_details["current_state"]]
        },
        ReturnValues="UPDATED_NEW"
    )
    print("response", response)
    return response


### ====================================== End of Dynamo ============================###

### ====================================== AWS Step Function ========================###
def continue_with_send_task_success(task_token, next_state_input, from_state, to_state):
    print(f"continuing with task token success from state {from_state} to {to_state}", task_token, next_state_input)
    step_functions = boto3.client('stepfunctions', region_name='ap-southeast-1')
    print("step_functions", step_functions)
    response = step_functions.send_task_success(
        taskToken=task_token,
        output=json.dumps(next_state_input)
    )
    print("response", response)


def continue_with_send_task_failure(task_token, error, cause, from_state, to_state):
    print(f"continuing with task token success from state {from_state} to {to_state}", task_token, error, cause)
    step_functions = boto3.client('stepfunctions', region_name='ap-southeast-1')
    print("step_functions", step_functions)
    response = step_functions.send_task_failure(
        taskToken=task_token,
        error='to-be-implemented',
        cause='to-be-implemented'
    )
    print("response", response)


### ====================================== End of AWS Step Function ========================###

def change_job_progress_to_started(job_details):
    print("changing job progress to started")
    instance_id = job_details["instance_id"]
    request_id = job_details["request_id"]
    submitted_by = job_details["submitted_by"]
    now_time = datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).isoformat()
    job_details["updated_at"] = now_time
    job_details["progress"].append(
        f"[{now_time}]: Request-{request_id} submitted by {submitted_by} "
        f" has started on ec2 machine ${instance_id}")
    job_details["current_state"] = "started"
    job_details["started"] = {
        "entered_at": now_time
    }
    update_job_progress(job_details)


def change_job_progress_to_finished(job_details, status):
    print("changing job progress to finished")
    instance_id = job_details["instance_id"]
    request_id = job_details["request_id"]
    submitted_by = job_details["submitted_by"]
    now_time = datetime.now(pytz.timezone('Asia/Kuala_Lumpur')).isoformat()
    job_details["updated_at"] = now_time
    job_details["progress"].append(
        f"[{now_time}]: Request-{request_id} submitted by {submitted_by} "
        f" has finished with status {status} on ec2 machine ${instance_id}")
    job_details["current_state"] = "finished"
    job_details["finished"] = {
        "entered_at": now_time,
        "status": status
    }
    update_job_progress(job_details)


def get_task_token_with_job_details(instance_id, from_state):
    print(f"getting job details and task token for instance {instance_id} and from_state = {from_state}")
    task_token = None
    while task_token is None:
        print("task token is null, going to fetch job details by instance id")
        job_details = get_job_details(instance_id)
        print(f"found job details = {job_details}")
        if job_details is not None and job_details.get(from_state, None) is not None and job_details[from_state].get(
                "task_token", None) is not None:
            return job_details, job_details[from_state]["task_token"]
        else:
            if job_details is None:
                print(f"No job details found, sleeping 30 seconds")
            elif job_details.get(from_state, None) is None:
                print(f"No state info found in job {job_details} for state = {from_state}, sleeping 30 seconds")
            else:
                print(
                    f"No task token found in state_cont_details {job_details[from_state]} of job {job_details} "
                    f"for state = {from_state}, sleeping 30 seconds")
            time.sleep(30)


def continue_with_start_token(instance_id):
    print("continuing with start token, going to fetch job details and task token")
    # init state has the task token to continue from init to started
    job_details, task_token = get_task_token_with_job_details(instance_id, 'init')
    print("found job details ", job_details, "task token", task_token)
    change_job_progress_to_started(job_details)
    print("Updated job status successfully")
    next_state_input = {
        "rtsRequest": {
            "requestId": job_details["request_id"],
            "submittedBy": job_details["submitted_by"],
            "rtsInput": json.loads(job_details["rts_input"])
        },
        "instanceId": job_details["instance_id"],
        "jobStatus": "started"
    }
    print("next_state_input", next_state_input)
    continue_with_send_task_success(task_token, next_state_input, 'started', 'running')
    return job_details


def run_rts_job(inputs, request_id):
    print("Running RTS job with input", inputs)
    try:
        publish_job_status("job_started", {"request_id": request_id})
        print("Published begin job status")
    except Exception as err1:
        print("Error publishing message in sqs before RTS starts", err1)

    # Get optimised results convert to dataframe
    # It is important to *throw/raise* proper error from the main method when there is an error
    # to show the error status in the UI.
    json_output = main(order_id=inputs['order_id'], region=inputs['region'], date=inputs['date'])
    print("json_output", json_output)

    try:
        publish_job_status("job_finished_with_success", {"request_id": request_id, "result": json_output})
        print("Published end job status")
    except Exception as err2:
        print("Error publishing message in sqs after RTS ends", err2)


def continue_with_completion_token(instance_id, is_success):
    status = "success"
    if not is_success:
        status = "failure"

    job_details, task_token = get_task_token_with_job_details(instance_id, 'running')
    print("found job details ", job_details, "task token", task_token)
    change_job_progress_to_finished(job_details, status)
    print("Updated job status to normal termination successfully")
    next_state_input = {
        "rtsRequest": {
            "requestId": job_details["request_id"],
            "submittedBy": job_details["submitted_by"],
            "rtsInput": json.loads(job_details["rts_input"])
        },
        "instanceId": job_details["instance_id"],
        "jobStatus": f"finished-with-{status}"
    }
    print("next_state_input", next_state_input)
    if is_success:
        continue_with_send_task_success(task_token, next_state_input, 'running', 'finished-with-success')
    else:
        continue_with_send_task_failure(task_token, None, None, 'running',
                                        'finished-with-error')  # Todo: Need to fix this with proper error


def print_run_info(instance_id):
    try:
        git_commit = os.getenv("GIT_COMMIT_ID")
        build_time = os.getenv("INFO_APP_BUILD_DATE")
        print(f"Git commit {git_commit} deployed at {build_time} is running on instance {instance_id}")
    except Exception as e:
        print("Error printing build info", e)


if __name__ == '__main__':
    print("Execution started")
    print("Getting instance id from metadata service")
    metadata_response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
    _instance_id = metadata_response.text
    print("instance_id", _instance_id)
    print_run_info(_instance_id)

    job_details = None
    try:
        job_details = continue_with_start_token(_instance_id)
    except Exception as e:
        print("Error happened while running <continue_with_start_token> with instance id = {_instance_id}, error = ", e)
        raise e  # Do not continue with running rts, it will break the state machine transitions,
        # state machine will automatically timed out, then check the CW log for this instance

    _is_success = True
    try:
        _rts_input_raw = job_details["rts_input"]
        print("RTS input found from job details", _rts_input_raw)
        _request_id = job_details["request_id"]
        print("request id found from job details", _request_id)
        rts_input = json.loads(_rts_input_raw)
        print("RTS input ", rts_input)
        run_rts_job(rts_input, _request_id)
    except Exception as e:
        _is_success = False
        print("Error occurred while running rts job. error = ", e)

    try:
        print(f"Task completed with _is_success = {_is_success}")
        continue_with_completion_token(_instance_id, _is_success)
        print(f"Execution completed successfully,instance {_instance_id} will shutdown now")

    except Exception as e:
        print(
            f"Error happened while running <continue_with_completion_token> with instance id = {_instance_id}, error = ",
            e)
        print(f"Execution completed with error,instance {_instance_id} will shutdown now")
        raise e  # state machine will automatically timed out, then check the CW log for this instance
