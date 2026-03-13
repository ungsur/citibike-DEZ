from airflow.sdk import dag, task
from datetime import datetime
@dag(schedule="@daily", start_date=datetime(2025, 1, 1))
def my_dag():
    
    @task
    def training_model(accuracy: int):
        return accuracy
    
    @task.branch
    def choose_best_model(accuracies: list[int]):
        if max(accuracies) > 2:
            return 'accurate'
        return 'inaccurate'
    
    @task.bash
    def accurate():
        return "echo 'accurate'" 
    
    @task.bash
    def inaccurate():
        return "echo 'inaccurate'"    

    accuracies = training_model.expand(accuracy=[1, 2, 3])
    choose_best_model(accuracies) >>   [accurate(), inaccurate()]
    
    
my_dag()