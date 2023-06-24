from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import json
import psycopg2
from helpers.database_helpers import get_db_engine, create_engine, make_url
import pandas as pd
from sqlalchemy import text
from configparser import ConfigParser

#alter table ddrr."scoreHistoryMethodResultTest" REPLICA IDENTITY DEFAULT ;

CONFIG = ConfigParser()
CONFIG.read("source_config.ini")

USERNAME = CONFIG["SOURCE_DEV"]["USERNAME"]
PASSWORD = CONFIG["SOURCE_DEV"]["PASSWORD"]
HOST = CONFIG["SOURCE_DEV"]["HOST"]
DB = CONFIG["SOURCE_DEV"]["DB"]

KAFKA_USERNAME=CONFIG["MSK"]["USERNAME"]
KAFKA_PASSWORD=CONFIG["MSK"]["PASSWORD"]

engine = get_db_engine()

SCHEMA_TABLE_MAPPING = {
        "a2v_data": ["findings"],
        "zoominfo": ["zoominfo_json"],
        "ddrr": [
            "scoreTextOrganization",
            "scoreTextCategoryTemplate",
            "ddrr_containerOrganization",
            "ddrr_container",
            "scoreCountryCode",
            "scoreQuestion",
            "scoreIndustry",
            "ddrr_naicsInherentRiskIndustry",
            "scoreCategory",
            "scoreHistoryMethodResult",
            "scoreContext",
            "scoreMethodAnswer",
            "scoreMethod",
            "ddrr_manualReviewIndustryOption",
            "ddrr_manualReviewResult",
            "ddrr_organization_country",
            "ddrr_organization",
            "score_client_version_history",
        ],
        "related_entity_discovery": ["banned_parent"],
    }

def get_primary_keys():
    

    # Create the tables_to_check dictionary
    result_df = pd.DataFrame()

    # Iterate over the schema and table mapping
    for schema, tables in SCHEMA_TABLE_MAPPING.items():
        for table in tables:
            # Retrieve primary key information and append it to the DataFrame
            df = get_primary_key(schema, table)
            df["schema"] = schema
            df["table"] = table
            result_df = pd.concat([result_df, df], ignore_index=True)

    result_dict = {}

    # Iterate over each row in the DataFrame
    for _, row in result_df.iterrows():
        schema_table = f"{row['schema']}.{row['table']}"
        primary_key = row["primary_key"]

        # Add schema_table as key and primary_key as value to the dictionary
        result_dict[schema_table] = primary_key


def get_connection_to_dddb():
    url_string = f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:5432/{DB}"
    return create_engine(make_url(url_string))


def get_primary_key(schema, table):
    dddb_engine = get_connection_to_dddb()

    with dddb_engine.connect() as conn:
        query = f"""
            SELECT column_name AS primary_key
            FROM information_schema.table_constraints
                JOIN information_schema.key_column_usage
                    USING (constraint_catalog, constraint_schema, constraint_name,
                            table_catalog, table_schema, table_name)
            WHERE constraint_type = 'PRIMARY KEY'
            AND (table_schema, table_name) = ('{schema}', '{table}')
            ORDER BY ordinal_position;
            """
        df = pd.read_sql_query(query, conn)

        df["schema"] = schema
        df["table"] = table

        return df


def execute_sql(sql):
    # Assuming you have a PostgreSQL database connection
    with engine.connect() as conn:
        try:
            print(f"Executiong SQL Statment: {sql}")
            conn.execute(text(sql))
            conn.commit()
            print("SQL statement executed successfully:", sql)
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print("Error executing SQL statement:", error)

# For each table, find the columns and datatype from postgresql

def generate_insert_sql(table, data):

    cn = []
    cv = []
    for k, v in data.items():
        cn.append(k)
        if v == "'None'" or v == 'None' or v is None:
            cv.append("NULL")
        else:
            cv.append(v)

    columns = ", ".join([f'"{k}"' for k in cn])
    values = ", ".join(cv)
    
    if '"zoominfo"."zoominfo_json"' in table:
        sql = f"INSERT INTO {table} ({columns}) OVERRIDING SYSTEM VALUE VALUES ({values})"
    else:
        sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"
    return sql

#Ensure all tables have a primary key set in the source

#manual process for DDL
def generate_update_sql(table, key_payload, after_date):
    assert key_payload is not None, "Update can't function without primary key"
    where_conditions = []
    for k, v in key_payload.items():
        where_conditions.append(f'"{k}" = \'{v}\'')

    update_parts = []
    for cn, cv in after_date.items():
        if cv == "\'None\'":
            update_parts.append(f'"{cn}" = NULL')
        else:
            update_parts.append(f'"{cn}" = {cv}')

    update_clause = ", ".join(update_parts)

    assert len(where_conditions) > 0, "For updates, we need to have before data in the event."

    where_clause = " AND ".join(where_conditions)

    sql = f"UPDATE {table} SET {update_clause} WHERE {where_clause}"
    return sql

def generate_delete_sql(table, key_payload):
    assert key_payload is not None, "Update can't function without primary key"
    # Assuming data is a dictionary where keys represent column names and values represent values
    where_clause = " AND ".join(
        [f'"{cn}" = \'{cv}\'' for cn, cv in key_payload.items()]
    )
    sql = f"DELETE FROM {table} WHERE {where_clause}"
    return sql


def _build_consumer():
    #username and password need to be in  a config file.
    conf = {
        "bootstrap.servers": "b-1.kafkadev.uyghh9.c2.kafka.us-west-2.amazonaws.com:9096",
        "group.id": "test8-sync_consumer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": KAFKA_USERNAME,
        "sasl.password": KAFKA_PASSWORD
    }
    print('Setting up Consumer')
    consumer = Consumer(conf)
    return consumer

'''
Executiong SQL Statment: INSERT INTO "ddrr"."score_client_version_history" ("id", "organization_id", "container_id", "total_score", "ddrr_version", "client_score_date", "created_at", "quick_scan") VALUES ('40568', '2666', '9', '792', '1.000000', to_timestamp(1609372800000000 / 1000000), '2021-07-06T13:13:19.959006Z', 'False')
Error executing SQL statement: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "score_client_version_history_pkey"
DETAIL:  Key (id)=(40568) already exists.

Debezium gets all the chances from a long time ago.  It's okay to skip the inserts.  Some events that will come agagin for the same record


Debezium snapshot start date?  To change update
'''

import json


def validate_json(json_str):
    try:
        json_str = json_str.replace("'", "''")
        #json_str = json_str.replace("'", "\"")
        escaped_json_str = json.dumps(json.loads(json_str))
        return escaped_json_str
    except json.JSONDecodeError as e:
        # Handle invalid JSON string
        print("Invalid JSON string:", e)
        return json_str
    
#turned off generated always
def _type_transformer(schema, data):
    print('in transforming function')
    for sc in schema:
        fn = sc["field"]

        deb_type = sc.get("name", "__default")
        if fn in data.keys() and fn is not None:
            if deb_type == 'io.debezium.data.Json':
                print("JSON Type")
                if data[fn] is not None:
                    json_str = validate_json(data[fn])
        
                    data[fn] = f"'{json_str}'"
                else:
                    data[fn] =  "NULL"
            elif deb_type == "io.debezium.time.MicroTimestamp":
                data[fn] = f"to_timestamp({data[fn]} / 1000000)"
            elif type(data[fn]) == str and ("'" in data[fn] or '"' in data[fn]):
                # import pdb; pdb.set_trace();
                str_data = data[fn]
                str_data = str_data.replace("'", r"''")
                str_data = str_data.replace('"', r'""')
                data[fn] = f"'{str_data}'"
            else:
                data[fn] = f"'{data[fn]}'"
    return data

def _event_processor(event):
    try:
        print("processing msg")
        event_value= event.value()
        if event_value is None:
            return
        value = json.loads(event_value.decode("utf-8"))
        operation = value["payload"]["op"]

        if operation == "r":
            if value["payload"]["source"]["snapshot"]:
                # modify snapshots as create
                operation = "c"
            else:
                return

        # print(f"Read Msg Value: {value}", flush=True)
        #schema=value['schema']["fields"]

        #fields_mapping = get_fields_to_cast(schema)
        change_schema = value.get("schema", {}).get("fields", [{}])
        before_schema = None
        after_schema = None
        for cs in change_schema:
            if cs["field"] == "before":
                before_schema = cs["fields"]
            if cs["field"] == "after":
                after_schema = cs["fields"]
        before_data = value["payload"]["before"]
        after_data = value["payload"]["after"]
        if before_data is not None:
            before_data = _type_transformer(before_schema, before_data)

        if after_data is not None:
            after_data = _type_transformer(after_schema, after_data)
        
        change_source = value["payload"]["source"]
        target_schema = change_source["schema"]
        target_table = change_source["table"]
        qualified_table_name = f'"{target_schema}"."{target_table}"'

        if operation in ["u", "d"]:
            key_payload = json.loads(event.key().decode("utf-8")).get("payload", None)

        if operation == "c":
            print("INSERT")
            statement = generate_insert_sql(qualified_table_name, after_data)

        elif operation == "u":
            print("UPDATE")
            statement = generate_update_sql(qualified_table_name, key_payload, after_data)

        elif operation == "d":
            print("DELETE")
            statement = generate_delete_sql(qualified_table_name, key_payload)

        execute_sql(statement)

    except Exception as e:
        print("Error with processing: ", e)


def consume_loop(consumer, topics):
    MIN_COMMIT_COUNT = 10
    running = True
    try:
        print(f'running for: {topics[0]}')
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(
                    f"Offsets: {msg.topic()}:{msg.partition()}:{msg.offset()}",
                    flush=True,
                )
                try:
                    _event_processor(msg)
                    consumer.commit(asynchronous=False)
                except Exception as e:
                    print(f'Error: {e}')
                    return
                
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


import click


@click.command()
@click.option("--table", help="Name of the table")
def main(table):
    # "dddb.ddrr.scoreHistoryMethodResult"
    consumer = _build_consumer()
    consume_loop(consumer, [table])


if __name__ == "__main__":
    main()
