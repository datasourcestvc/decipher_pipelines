FROM ghcr.io/dbt-labs/dbt-snowflake:1.4.1

COPY forsta_decipher/ .

COPY docker/run_commands.sh .

ENTRYPOINT ["/bin/bash", "run_commands.sh"]