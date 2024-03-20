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
def prepare_flats_buildings_dataset():
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
        flats_buildings_table = Table(
            'flats_buildings',
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
            UniqueConstraint('flat_id', name='unique_flat_id_constraint')
            )

        if not inspect(conn).has_table(flats_buildings_table.name):
            metadata.create_all(conn)

    @task()
    def extract():

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select f.id as flat_id
            , f.floor
            , f.is_apartment
            , f.kitchen_area
            , f.living_area
            , f.rooms
            , f.studio
            , f.total_area
            , f.price
            , f.building_id
            , b.build_year
            , b.building_type_int
            , b.latitude
            , b.longitude
            , b.ceiling_height
            , b.flats_count
            , b.floors_total
            , b.has_elevator
        from flats as f
        left join buildings as b
            on f.building_id=b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data['is_apartment'] = data['is_apartment'].astype('int')
        data['studio'] = data['studio'].astype('int')
        data['has_elevator'] = data['has_elevator'].astype('int')
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_buildings",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_flats_buildings_dataset()