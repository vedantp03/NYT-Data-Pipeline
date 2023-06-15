FROM python:3.11

ADD /dags/my_dag.py .

RUN pip install requests
RUN pip install apache-airflow

ENV PORT=8080

EXPOSE 8080

CMD ["python", "dags/my_dag.py"]