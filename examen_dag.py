from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
from datetime import datetime,timedelta
import json
from json import JSONDecodeError
import os
import pandas as pd

import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

import shutil

my_dag = DAG(
    dag_id='examen_predict_meteo',
    description='',
    tags=['examen', 'datascientest'],
    schedule_interval=timedelta(seconds=30),
    catchup=False,
     doc_md="""DAG permettant la recuperation de donnee meteologique et d'entrainement de modeles de prédiction de temperature""",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(year=2023,month=11,day=1),
    }
)

Variable.set(key="cities", value='["paris", "london", "washington","nantes"]')
path_to_model='/app/clean_data/{model_name}.pckl'


def fetch_data():
    url = "https://api.openweathermap.org/data/2.5/weather?q={c}&appid=04ee1d2a86aafaef8b198f7da3a867f9"

    all_cities = Variable.get(key="cities",deserialize_json=True)
    print(all_cities, type(all_cities))
    data = []
    for city in all_cities:
        response = requests.get(url.format(c=city))
        data.append(response.json())
    json_data = json.dumps(data,indent=2)
       
    
    time = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
    with open(f"/app/raw_files/{time}.json","w") as f:
            f.write(json_data)
        
def transform_data_into_csv(n_files=None, filename='data.csv'):
    parent_folder = '/app/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        try:
            with open(os.path.join(parent_folder, f), 'r') as file:
            
                    data_temp = json.load(file)
               
                    
            for data_city in data_temp:
           
                dfs.append(
                    {
                        'temperature': data_city['main']['temp'],
                        'city': data_city['name'],
                        'pression': data_city['main']['pressure'],
                        'date': f.split('.')[0]
                    }
                )
        except KeyError:
            print(f"Il y a souci avec le fichier {f}, il sera ignorer")
        except JSONDecodeError:
            print(f"Il y a souci avec le fichier {f},il sera ignorer")
            
        
    df = pd.DataFrame(dfs)

    print('\n', df.head(10))

    df.to_csv(os.path.join('/app/clean_data', filename), index=False)        

def debut():
    print("Debut")


def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score


def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    # training the model
    model=model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)
    print(str(model))
    return model


def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score    

def train_eval_model_lr(task_instance):
    model = LinearRegression()
    X,y =prepare_data()
    model =train_and_save_model(model,X,y,path_to_model=path_to_model.format(model_name='linear_regress'))
    score = compute_model_score(model,X,y)
    task_instance.xcom_push(
        key="data_lr",
        
        value={"score":score,"model_path":path_to_model.format(model_name='linear_regress')}
    )

def train_eval_model_dtr(task_instance):
    model = DecisionTreeRegressor()
    X,y = prepare_data()
    model = train_and_save_model(model,X,y,path_to_model=path_to_model.format(model_name='decision_tree_regress'))
    score = compute_model_score(model,X,y)
    task_instance.xcom_push(
        key="data_dtr",
        
        value={"score":score,"model_path":path_to_model.format(model_name='decision_tree_regress')}
    )

def train_eval_model_rfr(task_instance):
    model = RandomForestRegressor()
    X,y =prepare_data()
    model =train_and_save_model(model,X,y,path_to_model=path_to_model.format(model_name='random_forest_regress'))
    
    print("######",type(model))
    
    score = compute_model_score(model,X,y)
    task_instance.xcom_push(
        key="data_rfr",
        
        value={"score":score,"model_path":path_to_model.format(model_name='random_forest_regress')}
    )
def pick_the_best_model(task_instance):
    model_short_name = ['lr','rfr','dtr']
    max_score = -100000000
    best_model_path = "'/app/clean_data/best_model.pickle'"
    for msn in model_short_name:
        data = task_instance.xcom_pull(key="data_{}".format(msn),task_ids="train_{}".format(msn))
        if data:
            if data["score"] > max_score:
                max_score = data["score"]
                best_model_path = data["model_path"]
    print("Le meilleur model est le model ",best_model_path )
    shutil.copy2(best_model_path,'/app/clean_data/best_model.pickle')
    
    
    

start = PythonOperator(task_id="start",
                          python_callable=debut,
                          doc_md="""Tache de début de dag """,
                          dag=my_dag)

get_data = PythonOperator(task_id="get_data",
                          python_callable=fetch_data,
                          doc_md=""" Tache de recuperation des donnees via l'api OpenWeatherMap => creation de json pour stocker les donnée""",
                          dag=my_dag)


create_data =  PythonOperator(task_id="create_data",
                          python_callable=transform_data_into_csv,
                          op_kwargs={"n_files":20, "filename":'data.csv'},
                          doc_md=""" Tache de transformation des donnees json en fichier CSV avec seulement les 20 dernier données pour chaques villes""",

                          dag=my_dag)
create_full_data = PythonOperator (task_id="create_full_data",
                          python_callable=transform_data_into_csv,
                          op_kwargs={"n_files":None, "filename":'fulldata.csv'},
                          doc_md=""" Tache de transformation de toutes les donnees  json en fichier CSV """,
                          dag=my_dag)

train_lr = PythonOperator (task_id="train_lr",
                          python_callable=train_eval_model_lr,
                          doc_md=f"""Entrainement d'un modele Linearressor avec toutes les donnee""",
                          dag=my_dag)
train_dtr = PythonOperator (task_id="train_dtr",
                          python_callable=train_eval_model_dtr,
                          doc_md=f"""Entrainement d'un modele decisionTreeRegressor avec toutes les donnee""",
                          dag=my_dag)
train_rfr = PythonOperator (task_id="train_rfr",
                          python_callable=train_eval_model_rfr,
                          doc_md=f"""Entrainement d'un modele randomForestRegressor avec toutes les donnee""",
                          dag=my_dag)

pick_one_model = PythonOperator (task_id="pick_one_model",
                          python_callable=pick_the_best_model,
                          trigger_rule='all_done',
                          doc_md=f"""Choix du meilleur modele a partir des score sauvegarder dans Xcoms""",
                          dag=my_dag)

start >> get_data
get_data >> create_data
get_data >> create_full_data
create_full_data >> [train_lr,train_dtr,train_rfr]
[train_lr,train_dtr,train_rfr] >> pick_one_model
