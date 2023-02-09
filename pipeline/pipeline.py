from pipelines import tasks, Pipeline
from pipelines.tasks import (
    sql_create_table_as, 
    load_file_to_db, 
    save_table_to_file
)

NAME = 'test_project'
SCHEMA = 'public'
YEAR_SUFFIX = '2023'
LOAD_TASK = load_file_to_db(input='original/original.csv', output='original')

TASKS = [
        LOAD_TASK,
        sql_create_table_as(
            table='norm',
            query='''
                select *, domain_of_url(url)
                from {original};
            '''
        ),
        tasks.CopyToFile(
            input='norm',
            output='norm',
        ),

        # clean up:
        sql('drop table {original}'),
        sql('drop table {norm}'),
    ]


pipeline = Pipeline(
    name=NAME,
    schema=SCHEMA,
    version=VERSION,
    tasks=TASKS
)

if __name__== "__main__":
    LOAD_TASK.run() -- run particular task
    pipline.run() -- run pipelinres of tasks
    