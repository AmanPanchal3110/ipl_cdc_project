import requests
import json
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("connector_setup")

conn_url="http://localhost:8083/connectors"

conn_conf={
    "name":"ipl-postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "Aman",
        "database.password": "panchal",
        "database.dbname": "cdc_db",
        "topic.prefix": "ipl_cdc",
        "table.include.list": "public.matches,public.deliveries",
        "topic.creation.default.partitions": "3",
        "topic.creation.default.replication.factor": "1",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "debezium_publication",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "false",
            }
}


def wait_for_connect(retries=10,delay=5):
    log.info("wait for kafka connect ready")
    for attempts in range(1,retries+1):
        try:
            r=requests.get(conn_url)
            if r.status_code==200:
                log.info("kafka connect ready hai")
                return True
        except requests.exceptions.ConnectionError:
            log.warning(f"Attempt {attempts}")
            time.sleep(delay)
    log.error("Timeout — Kafka Connect ready nahi hua")
    return False
    
def connection_exists():
    r=requests.get(f"{conn_url}/ipl-postgres-connector")
    return r.status_code==200

def regrister_conn():
    return requests.post(
        conn_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(conn_conf)
    )
    
def check_status():
    return requests.get(f"{conn_url}/ipl-postgres-connector/status").json()

def main():
    if not wait_for_connect():
        return
    
    if connection_exists():
        log.info("Already registered — status:")
        log.info(json.dumps(check_status(), indent=2))
        return
    
    log.info("connector register")
    r=regrister_conn()
    
    if r.status_code in (200,201):
        time.sleep(3)
        status=check_status()
        c_state = status["connector"]["state"]
        t_state = status["tasks"][0]["state"] if status["tasks"] else "NO TASKS"
        log.info(f"Connector: {c_state} | Task: {t_state}")
        if c_state == "RUNNING" and t_state == "RUNNING":
            log.info("CDC shuru ho gaya!")
            log.info("Kafka UI: http://localhost:9090")
        else:
            log.error(json.dumps(status, indent=2))
    else:
        log.error(f"Fail — {r.status_code}: {r.text}")

if __name__ == "__main__":
    main()