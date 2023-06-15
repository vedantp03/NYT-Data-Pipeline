from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


import smtplib
from requests import get
from datetime import datetime

default_email_reipients = "vedant3patel@gmail.com"
default_recipients = Variable.get(default_email_reipients, default_var=None)

def send(ti):
    # This code only send an email if the day of the week is Friday
    if datetime.now().weekday() == 4:
        data = ti.xcom_pull(key="newyorktimes_data", task_ids=["newyorktimes_data"])
        try:
            recipients = ti.xcom_pull(key=None, task_ids=["extra_recipients"])
            recipients = recipients or default_recipients
            recipients.split(",") if recipients else []
            x = smtplib.SMTP("smtp.gmail.com", 587)
            x.starttls()
            x.login("compalternate@gmail.com", "pteiewqayfmxkqef")
            subject="New York Times Weekly Best Sellers"
            message=f"Subject: {subject}\n\n{data}"
            x.sendmail("compalternate@gmail.com", "vedant3patel@gmail.com" if not recipients else recipients, message)
            print("success")
        except Exception as e:
            print(e)
            print("failure to send")
            pass
    else:
        print("TASK ALREADY COMPLETED FOR THE WEEK")

# Defaults arguments for Dags
default_args = {
    'owner': 'Vedant Patel',
    'start_date': datetime(2023, 6, 14),
}

# Constructor for all of my dags
dag = DAG(
    dag_id='nytimes_bestsellers',
    default_args=default_args,
    schedule_interval='@daily',
)

# This function returns a labeled 5 point list of the top 5 Nonfiction & Fuction books from the NYT
def get_data(ti):
    key = "7VHOflT5dvcwMce5ZJbZhw4mIMXWNDwC"
    response = (get(f"https://api.nytimes.com/svc/books/v3/lists/overview.json?q=election&api-key={key}"))
    data = response.json()
    return_str = ""
    for n, el in enumerate(data["results"]["lists"][0]["books"]):
        return_str += (f"{n + 1}: {el['title']}  ")
    ti.xcom_push(key="newyorktimes_data", value=return_str)
    return return_str

# Grabs any extra recipients that may have been overridden from the airflow config
def extra_recipients(**context):
    xtra_recipients = Variable.get("extra_email_recipients", default_var=None)
    context["ti"].xcom_push(key="extra_recipients", value=xtra_recipients)

# Task in charge of calling the function above
get_extra_recipients_task = PythonOperator(
    task_id = "get_extra_recipients",
    python_callable=extra_recipients,
    provide_context = True,
    dag=dag,
)

# Task in charge of getting the NYT top 5 books
newyorktimes_data = PythonOperator(
    task_id = "newyorktimes_data",
    python_callable = get_data,
    dag = dag,
)

# Tasks in charge of sending emails out to default recipients or overrideen recipients
send_email_task = PythonOperator(
    task_id="send_email",
    python_callable= send,
    provide_context = True,
    dag=dag,
)

# Pathway for AirFlow
newyorktimes_data >> get_extra_recipients_task >> send_email_task 