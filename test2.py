from prefect import flow, task
import pandas as pd

@task
def df_name():
     # assign data of lists.
    data = {'name': ['wikipedia', 'google', 'mail'], 'url': ['https://ru.wikipedia.org/wiki/%D0%AF%D0%BD%D0%B4%D0%B5%D0%BA%D1%81u', 'https://www.google.com/intl/ru_ru/drive/', 'https://pogoda.mail.ru/prognoz/saratov/']}
    # Create DataFrame
    df = pd.DataFrame(data)
    df.to_csv('df_name-1.csv')
    return df
    
@flow(log_prints=True, name="prefect-example-hello-flow")
def df_result():
    df_result = df_name()
    df_result['domain_of_url'] = df_result['url'].map(lambda x: str(x).partition("://")[2].partition("/")[0])
    df_result.to_csv('df_result-1.csv')
    
    return df_result
if __name__ == "__main__":
    df_result()