FROM apache/airflow:2.4.0



RUN pip install --no-cache-dir apache-airflow-providers-slack
RUN pip install --no-cache-dir apache-airflow-providers-airbyte
RUN pip install --no-cache-dir apache-airflow-providers-snowflake
RUN pip install --no-cache-dir apache-airflow-providers-databricks
RUN pip install --no-cache-dir apache-airflow-providers-amazon
RUN pip install --no-cache-dir decipher

# Install dbt-snowflake in a virtual environment to avoid conflict
USER root
ENV PIP_USER=false
RUN python -m venv /usr/local/airflow/dbt_env
RUN /usr/local/airflow/dbt_env/bin/pip install --no-cache-dir pandas
RUN /usr/local/airflow/dbt_env/bin/pip install --no-cache-dir requests
RUN /usr/local/airflow/dbt_env/bin/pip install --no-cache-dir fastparquet
#RUN /usr/local/airflow/dbt_env/bin/pip install --no-cache-dir decipher 
ENV PIP_USER=true

USER airflow