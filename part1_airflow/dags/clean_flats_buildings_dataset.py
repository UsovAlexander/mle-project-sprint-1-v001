import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_flats_buildings_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():
        import sqlalchemy
        from sqlalchemy import Table, MetaData, Column, Integer, String, Float, Boolean, DateTime, UniqueConstraint, inspect, BigInteger
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()
        
        metadata = MetaData()
        clean_flats_buildings_table = Table(
            'clean_flats_buildings',
            metadata,
            Column('flat_id', BigInteger, primary_key=True, autoincrement=True),
            Column('floor', Integer),
            Column('is_apartment', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('studio', Integer),
            Column('total_area', Float),
            Column('price', BigInteger),
            Column('building_id', BigInteger),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Integer),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Integer),
            UniqueConstraint('flat_id', name='unique_clean_flat_id_constraint')
            )

        if not inspect(conn).has_table(clean_flats_buildings_table.name):
            metadata.create_all(conn)

    @task()
    def extract():

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select *
        from flats_buildings
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        def remove_duplicates(data: pd.DataFrame):
            feature_cols = data.columns.drop('flat_id').tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data 

        def fill_missing_values(data: pd.DataFrame):
            cols_with_nans = data.isnull().sum()
            cols_with_nans = cols_with_nans[cols_with_nans > 0].index
            mode_col = ['is_apartment', 'has_elevator', 'building_type_int', 'rooms', 'floor', 'build_year', 'flats_count']
            for col in cols_with_nans:
                if col not in mode_col:
                    fill_value = data[col].median()
                elif col in mode_col:
                    fill_value = data[col].mode().iloc[0]
                data[col] = data[col].fillna(fill_value)
            return data 

        def remove_outliers(data: pd.DataFrame):
            outliers_cols = ['kitchen_area', 'living_area', 'total_area', 'price', 'ceiling_height']
            threshold = 1.5
            potential_outliers = pd.DataFrame()

            for col in outliers_cols:
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                margin = threshold * IQR
                lower = Q1 - margin
                upper = Q3 + margin
                potential_outliers[col] = ~data[col].between(lower, upper)

            outliers = potential_outliers.any(axis=1)
            data = data[~outliers].reset_index(drop=True)

            return data

        rm_dp_data = remove_duplicates(data)
        fl_mv_data = fill_missing_values(rm_dp_data)
        data = remove_outliers(fl_mv_data)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="clean_flats_buildings",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
clean_flats_buildings_dataset()