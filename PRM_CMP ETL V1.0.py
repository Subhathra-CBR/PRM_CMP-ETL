import psycopg2
import json
import os

from datetime import datetime
from dotenv import load_dotenv

# --------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

load_dotenv()

required_vars = [
    "PRM_HOST",
    "PRM_DB",
    "PRM_USER",
    "PRM_PASSWORD",
    "CMP_HOST",
    "CMP_DB",
    "CMP_USER",
    "CMP_PASSWORD"
]

missing = [v for v in required_vars if not os.getenv(v)]

if missing:
    raise ValueError(
        f"Missing environment variables: {', '.join(missing)}"
    )

# --------------------------------------------------
# DATABASE CONFIG
# --------------------------------------------------

PRM_CONFIG = {
    "host": os.getenv("PRM_HOST"),
    "database": os.getenv("PRM_DB"),
    "user": os.getenv("PRM_USER"),
    "password": os.getenv("PRM_PASSWORD")
}

CMP_CONFIG = {
    "host": os.getenv("CMP_HOST"),
    "database": os.getenv("CMP_DB"),
    "user": os.getenv("CMP_USER"),
    "password": os.getenv("CMP_PASSWORD")
}


# --------------------------------------------------
# CONNECTION
# --------------------------------------------------

def get_connection(config):
    return psycopg2.connect(**config)

# --------------------------------------------------
# WATERMARK FUNCTIONS
# --------------------------------------------------

def get_watermark(cmp_conn, pipeline_name):

    cur = cmp_conn.cursor()

    cur.execute("""
        SELECT last_processed_timestamp
        FROM etl.etl_watermark
        WHERE pipeline_name = %s
    """, (pipeline_name,))

    row = cur.fetchone()

    if row:
        return row[0]

    return datetime(1970, 1, 1)


def update_watermark(
        cmp_conn,
        pipeline_name,
        latest_timestamp,
        processed_count):

    cur = cmp_conn.cursor()

    cur.execute("""
        INSERT INTO etl.etl_watermark (
            pipeline_name,
            last_processed_timestamp,
            last_run_timestamp,
            status,
            records_processed
        )
        VALUES (
            %s,
            %s,
            NOW(),
            'SUCCESS',
            %s
        )
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
            last_processed_timestamp =
                EXCLUDED.last_processed_timestamp,
            last_run_timestamp = NOW(),
            status = 'SUCCESS',
            records_processed =
                EXCLUDED.records_processed
    """,
    (
        pipeline_name,
        latest_timestamp,
        processed_count
    ))

    cmp_conn.commit()

# --------------------------------------------------
# PARTICIPANT ETL
# --------------------------------------------------

def extract_participant_delta(prm_conn, watermark):

    query = """
    SELECT
        p.id,
        p.cbr_barcode,
        p.cohort_id,
        p.date_of_birth,
        p.sex,
        p.recruitment_date,
        p.recruited_by,
        p.created_at,
        p.updated_at
    FROM participants p
    WHERE p.updated_at > %s
      AND p.id IN (1132,1133)
    ORDER BY p.updated_at
    """

    cur = prm_conn.cursor()

    cur.execute(query, (watermark,))

    return cur.fetchall()


def load_participants_cmp(cmp_conn, participant_rows):

    upsert_sql = """
    INSERT INTO cohort_101.participants (
        id,
        cbr_barcode,
        cohort_barcode,
        cohort_short_id,
        gender,
        date_of_birth,
        language,
        start_date,
        created_at,
        created_by,
        updated_at,
        is_deleted
    )
    VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )

    ON CONFLICT (id)
    DO UPDATE SET
        cbr_barcode      = EXCLUDED.cbr_barcode,
        cohort_barcode   = EXCLUDED.cohort_barcode,
        cohort_short_id  = EXCLUDED.cohort_short_id,
        gender           = EXCLUDED.gender,
        date_of_birth    = EXCLUDED.date_of_birth,
        start_date       = EXCLUDED.start_date,
        created_by       = EXCLUDED.created_by,
        updated_at       = EXCLUDED.updated_at
    """

    cur = cmp_conn.cursor()

    latest_timestamp = None

    for row in participant_rows:

        participant_id = row[0]
        cbr_barcode = row[1]

        insert_row = (
            participant_id,                          # id
            cbr_barcode,                             # cbr_barcode
            cbr_barcode,                             # cohort_barcode
            str(row[2]),                             # cohort_short_id
            row[4],                                  # gender
            row[3],                                  # date_of_birth
            json.dumps({}),                          # language
            row[5],                                  # start_date
            row[7],                                  # created_at
            str(row[6]) if row[6] else None,         # created_by
            row[8],                                  # updated_at
            False                                    # is_deleted
        )

        cur.execute(upsert_sql, insert_row)

        latest_timestamp = row[8]

    cmp_conn.commit()

    return latest_timestamp


def sync_participants(prm_conn, cmp_conn):

    watermark = get_watermark(
        cmp_conn,
        'prm_cmp_participant_sync'
    )

    participants = extract_participant_delta(
        prm_conn,
        watermark
    )

    if not participants:
        print("No participant changes found")
        return

    latest_timestamp = load_participants_cmp(
        cmp_conn,
        participants
    )

    update_watermark(
        cmp_conn,
        'prm_cmp_participant_sync',
        latest_timestamp,
        len(participants)
    )

    print(
        f"{len(participants)} participant rows processed"
    )

# --------------------------------------------------
# VISIT ETL
# --------------------------------------------------

def extract_visit_delta(prm_conn, watermark):

    query = """
    SELECT
        v.id,
        v.participant_id,
        v.created_by,
        v.window_start_date,
        v.window_end_date,
        v.updated_at
    FROM visits v
    WHERE v.updated_at > %s
      AND v.participant_id IN (1132,1133)
    ORDER BY v.updated_at
    """

    cur = prm_conn.cursor()

    cur.execute(query, (watermark,))

    return cur.fetchall()


def load_visits_cmp(cmp_conn, visit_rows):

    upsert_sql = """
    INSERT INTO cohort_101.visit_occurrences (
        id,
        participant_id,
        created_by,
        visit_start_date,
        visit_end_date,
        updated_at
    )
    VALUES (%s,%s,%s,%s,%s,%s)

    ON CONFLICT (id)
    DO UPDATE SET
        participant_id   = EXCLUDED.participant_id,
        created_by       = EXCLUDED.created_by,
        visit_start_date = EXCLUDED.visit_start_date,
        visit_end_date   = EXCLUDED.visit_end_date,
        updated_at       = EXCLUDED.updated_at
    """

    cur = cmp_conn.cursor()

    latest_timestamp = None

    for row in visit_rows:

        cur.execute(upsert_sql, row)

        latest_timestamp = row[5]

    cmp_conn.commit()

    return latest_timestamp


def sync_visits(prm_conn, cmp_conn):

    watermark = get_watermark(
        cmp_conn,
        'prm_cmp_visit_sync'
    )

    visits = extract_visit_delta(
        prm_conn,
        watermark
    )

    if not visits:
        print("No visit changes found")
        return

    latest_timestamp = load_visits_cmp(
        cmp_conn,
        visits
    )

    update_watermark(
        cmp_conn,
        'prm_cmp_visit_sync',
        latest_timestamp,
        len(visits)
    )

    print(
        f"{len(visits)} visit rows processed"
    )

# --------------------------------------------------
# MAIN ETL
# --------------------------------------------------

def run_etl():

    prm_conn = get_connection(PRM_CONFIG)
    cmp_conn = get_connection(CMP_CONFIG)

    try:

        sync_participants(
            prm_conn,
            cmp_conn
        )

        sync_visits(
            prm_conn,
            cmp_conn
        )

        print("ETL completed successfully")

    except Exception as ex:

        print(f"ETL Failed : {str(ex)}")

        cmp_conn.rollback()

    finally:

        prm_conn.close()
        cmp_conn.close()


if __name__ == "__main__":
    run_etl()