import pandas as pd
from dagster import asset, get_dagster_logger
from sklearn.model_selection import train_test_split
from catboost import Pool, CatBoostRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

@asset
def df_android() -> None:
    url = 'https://raw.githubusercontent.com/shanealynn/Pandas-Merge-Tutorial/master/android_devices.csv'
    df = pd.read_csv(url)
    df = df.rename(columns={'Retail Branding':'brand'})
    df.to_pickle('df_android.pkl')

@asset
def df_device():
    url = 'https://raw.githubusercontent.com/shanealynn/Pandas-Merge-Tutorial/master/user_device.csv'
    df = pd.read_csv(url)
    return df
 
@asset
def df_usage():
    url = 'https://raw.githubusercontent.com/shanealynn/Pandas-Merge-Tutorial/master/user_usage.csv'
    df = pd.read_csv(url)
    return df

@asset(non_argument_deps={"df_android"})
def df_result(df_device, df_usage):
    df_android = pd.read_pickle('df_android.pkl')
    df = (df_usage
        .merge(df_device[['use_id','platform','platform_version','device']], on='use_id')
        .merge(df_android[['Model','brand']], left_on='device', right_on='Model'))
    df = df.drop(columns=['use_id','device','Model','platform'])
    df.to_csv('df_name.csv')
    return df

@asset
def train_test(df_result):
    train_test = train_test_split(df_result, test_size=0.3, random_state=42)
    return train_test

@asset
def model(train_test):
    train, _ = train_test
    X_train, y_train = train.drop(columns='monthly_mb'), train['monthly_mb']
    model = CatBoostRegressor()
    model.fit(X_train, y_train, cat_features=['brand'], verbose=False)
    return model

@asset
def eval(model, train_test):
    _, test = train_test
    X_test, y_test = test.drop(columns='monthly_mb'), test['monthly_mb']
    y_pred = model.predict(X_test)    
    scores = {
        'r2': r2_score(y_test, y_pred),
        'MAE': mean_absolute_error(y_test, y_pred),
        'MSE': mean_squared_error(y_test, y_pred)
    }
    get_dagster_logger().info(scores)
    return scores