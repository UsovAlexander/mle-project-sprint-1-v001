# mle-project-sprint1-v001
Это репозиторий-шаблон для проекта Яндекс Практикума спринта №1.
Общая цель проекта: создать базовое решение для предсказания стоимости квартир сервиса "Своя Избушка"
Проект разделен на 3 этапа:
1. Сбор данных
    DAG с функциями создания общей таблицы называется flats_buildings.py и находится в папке /home/mle-user/mle_projects/mle-project-sprint-1-v001/part1_airflow/dags
2. Очистка данных
    Notebook c анализом датасета и функциями очистки называется clean_flats_buildings.ipynb и находится в папке /home/mle-user/mle_projects/mle-project-sprint-1-v001/part1_airflow/
    DAG с функциями очистки датасета называется clean_flats_buildings.py и находится по адресу /home/mle-user/mle_projects/mle-project-sprint-1-v001/part1_airflow/dags
3. Обучение модели
    Путь до файлов с Python-кодом для этапов DVC-пайплайна: home/mle-user/mle_projects/mle-project-sprint-1-v001/part2_dvc/scripts
    Путь до файлов с конфигурацией DVC-пайплайна dvc.yaml, params.yaml, dvc.lock: home/mle-user/mle_projects/mle-project-sprint-1-v001/part2_dvc/

Весь код, обеспечивающий решение 1 и 2 этапов, сохраните в папке этого репозитория под названием `part1_airlfow` <br/>
Весь код 3 этапа – `part2_dvc` <br/>
