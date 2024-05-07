import json
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['mwaa'])
def mwaa_flow():
    import random
    i_1 = 5
    f_1 = 1.28

    # use a kube_config stored in s3 dags folder
    kube_config_path = "/usr/local/airflow/dags/kube_config.yaml"

    # Func A is meant to run locally on Docker Compose
    # But MWAA is already running on ECS Fargate. In this case, I am running it as a pythons script locally on MWAA node
    # If this needs to run on container, use EKS or ECS/Batch based on customer container orchestration choice
    @task(multiple_outputs=True)
    def Func_A(inp_1: int, inp_2: float) -> dict: # NamedTuple is not JSON serialisable, replaced with dict
                                                  # See https://github.com/apache/airflow/discussions/34150
        import numpy as np
        inp_2 += np.random.random()
        
        # random permutation operation 
        def permute(nums):
            # DPS with swapping
            res = []
            if len(nums) == 0:
                return res
            get_permute(res, nums, 0)
            return res

        def get_permute(res, nums, index):
            if index == len(nums):
                res.append(list(nums))
                return
            for i in range(index, len(nums)):
                nums[i], nums[index] = nums[index], nums[i]
                # s(n) = 1 + s(n-1)
                get_permute(res, nums, index + 1)
                nums[i], nums[index] = nums[index], nums[i]

        output_1 = np.sum(np.array(permute([inp_1, inp_2])))
        from collections import namedtuple
        poutput = namedtuple("outputs", ['index', 'val', 'val2', 'val3'])
        return {
            "index": inp_1, 
            "val": output_1, 
            "val2": [output_1 + np.random.random() for k in range(int(3))], 
            "val3": [output_1 + np.random.random() for k in range(3)]
        }
    
    # Func_B is running on AWS Lambda
    # No TaskFlow API decorator for LambdaInvokeFunctionOperator
    # Ref: https://docs.astronomer.io/learn/airflow-decorators?tab=taskflow#available-airflow-decorators 
    # Note: xcom pull returns strings. This can be overridden with render_template_as_native_obj=True at DAG level but this 
    # messes up serialisation for payload. Hence we will sanitise in Lambda
    Func_B = LambdaInvokeFunctionOperator(
        task_id="Func_B",
        function_name="mwaa-lambda-demo-FuncB-MjJGjtkbyzrG",
        invocation_type="RequestResponse",
        log_type="Tail",
        payload=json.dumps({
            "inp_1": [i_1, "{{ ti.xcom_pull(task_ids='Func_A', key='index') }}", "{{ ti.xcom_pull(task_ids='Func_A', key='val') }}"], 
            "inp_2": f_1
        })
    )

    @task
    def Func_C_inputs(x):
        output = []
        output.append(json.loads(x))
        Func_C_runs = random.randint(4,8)
        print(f"Func_B output: {output} Type: {type(output)}")
        return output * Func_C_runs
    
    # Func C is running on Amazon EKS
    @task.kubernetes(
        image="<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/mwaa-ecr:latest",
        task_id="Func_C",
        cluster_context = "mwaa", # Must match kubeconfig context
        namespace = "mwaa",
        get_logs = True,
        is_delete_operator_pod = True,
        in_cluster = False,
        config_file = kube_config_path,
    )
    def Func_C(arg) -> dict:
        import numpy as np
        #json_arg = json.loads(arg)
        print(f"Func_C input: {arg}")
        inp_1 = 'hello' + str(arg["index"])
        inp_2 = arg["val"]
        inp_2 += np.random.random()
        print(f"inp_1: {inp_1}, inp_2: {inp_2}")
        output_1 = str(inp_2) + inp_1
        return {"index": inp_2, "val": output_1}
    
    @task
    def Func_D_inputs(m, g, h):
        Func_D_runs = random.randint(1,2)
        Func_D_inputs = []
        #g = Func_A
        h_json = json.loads(h)
        print(f"Func_C output: {m} Type: {type(m)}") # Type LazyXComAccess which you can loop over. 
        print(f"Func_B output: {h} Type: {type(h)}")
        print(f"Func_A output: {g} Type: {type(g)}")

        for j in range(Func_D_runs):
            Func_D_inputs.append({"inp_1": m[2 * j]["index"], "inp_2": i_1, "inp_3": g["val"], "inp_4": h_json["val"]})
        return Func_D_inputs
    
    # Func D is supposed to run on AWS Batch
    # Today with Batch operator you cannot send back data to Airflow xcom.
    # You will need a sepatrate step to parse the Batch logs to extract return value
    # ECS supports xcom push, so is an alternative. See second answer on SO post below for example
    # https://stackoverflow.com/questions/59372523/airflow-how-to-push-xcom-from-ecs-operator 
    # we will use EKS instead.
    @task.kubernetes(
        image="<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/mwaa-ecr:latest",
        task_id="Func_D",
        cluster_context = "mwaa", # Must match kubeconfig context
        namespace = "mwaa",
        get_logs = True,
        is_delete_operator_pod = True,
        in_cluster = False,
        config_file = kube_config_path,
    )
    def Func_D(arg):
        import numpy as np
        import random
        inp_1 = arg["inp_1"]
        inp_2 = arg["inp_2"]
        inp_3 = arg["inp_3"]
        inp_4 = arg["inp_4"]
        print(f"Func_D input: {inp_1}, {inp_2}, {inp_3}, {inp_4}")
        print(f"Func_D input type: {type(inp_1)}, {type(inp_2)}, {type(inp_3)}, {type(inp_4)}")
        inp_2 += np.random.random()
        # concatenate some strings
        output_1 = str(inp_2 + inp_3 + inp_4) + str(inp_1)
        
        poutput = {"index": inp_2, "val": output_1, "random_value": random.random()}
        return poutput
    
    @task_group
    def Func_E_group(arg):
        # Func E is running locally (no Docker) - simple passthrough function
        @task
        def Func_E(arg) -> dict:
            inp_1: float = arg["random_value"]
            return {"random_value": inp_1}
        
        @task
        def Func_E_child_inputs(x):
            Func_E_inputs = []
            for k in range(random.randint(0,3)):
                for i in range(2):
                    Func_E_inputs.append({"random_value": x["random_value"]})
            print(f"Func_E_child_inputs: {Func_E_inputs}")
            return Func_E_inputs
        
        Func_E_child_ip = Func_E_child_inputs(Func_E(arg))
        return Func_E_child_ip

    # Cannot have circular dependencies in DAG. Hence replicating this logic
    # We cannot pull this inside task group as operator expansion in expanded task group is not supported
    # we end up with a list of lists
    @task_group
    def Func_E_child_group(arg):
        @task
        def flatten_list(l):
            return [item for sublist in l for item in sublist]
        
        @task
        def Func_E_child(arg) -> dict:
            print(f"arg: {arg}")
            inp_1: float = arg["random_value"]
            return {"random_value": inp_1}
        
        flatten_op = flatten_list(l=arg)
        Func_E_child_op = Func_E_child.expand(arg=flatten_op)
        return Func_E_child_op

    func_a_op = Func_A(i_1, f_1) 
    func_a_op >> Func_B  # Func_B is a non TaskFlow operator. hence chained using >>
    func_c_op = Func_C.override(do_xcom_push=True).expand(arg=Func_C_inputs(Func_B.output))
    func_d_op = Func_D.override(do_xcom_push=True).expand(arg=Func_D_inputs(func_c_op, func_a_op, Func_B.output))
    func_e_op = Func_E_child_group(Func_E_group.expand(arg=func_d_op))

mwaa_flow()
