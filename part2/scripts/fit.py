# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from catboost import CatBoostRegressor
import yaml
import os
import joblib

# обучение модели
def fit_model():
	# Прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
	# загрузите результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
	# реализуйте основную логику шага с использованием гиперпараметров
    data = data.drop('building_id', axis=1)
    num_features = data.select_dtypes(['float'])

    preprocessor = ColumnTransformer(
        [
        ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='passthrough',
        verbose_feature_names_out=False
    )

    model = CatBoostRegressor()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data.drop(params['target_col'], axis=1), data[params['target_col']]) 
	# сохраните обученную модель в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 

if __name__ == '__main__':
	fit_model()