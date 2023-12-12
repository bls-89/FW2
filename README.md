# 1T final work

Финальная работа Соколовского Б.Л.(еще одна)

airflow/dags - папка содержащая в себе все файлы DAGов и вспомогательных файлов для выполнения поставленной в ТЗ задачи.

doc - папка с документами по проекту: ТЗ.docx - описание задания из LMS.  stocks.pptx - презентация PowerPoint по проекту.

docker-compose.yml - файл для создания и запуска Docker-контейнера с AirFlow для оркестрации и Postgres для размещения базы данных.





ИНСТРУКЦИЯ:

Выполнение позволяет сформировать требуемую в ТЗ витрину даных".

   •  развернуть Docker-контейнер с помощью файла docker-compose.yml
   
   •  подключиться к Airflow c помощью браузера по адресу http://localhost:8080/ 
   
   •	задействовать DAGи согласно их нумерации (01,02,03,04) DAGи под номерами 03 и 04 запускаются по расписанию! для демонстрации работы все расписания рекомендуется использование ручного запуска.
   
   •	c помощью DBeaver или другого иснтрумента подклочиться к базе данных Postgres используя следущие данные: хост-"localhost", порт -"5430", пользователь -"postgres",пароль - "password", база даных -"test".

В результате можно будет увидеть: 
- сформированные таблицы с историческими данными с именем вида "*****_full"
- таблицы, содержащие дельту за прошедшие сутки с именем вида "*****_daily"
- витрину с именем mart - содержащую аналитическую информацию за прошедшие сутки.
