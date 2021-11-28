#!/usr/bin/python
import psycopg2, pickle
from database.db_configurator import config

def connect_read(command):
    """ Connect to the PostgreSQL database server """
    conn = None
    response = None
    try:

        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)
        response = cur.fetchall()
       
        cur.close()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return response

def connect_write(command):
    """ Connect to the PostgreSQL database server """
    conn = None
    response = None
    try:

        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(command)

        conn.commit()
        cur.close()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def create_readings_table():
    
    sql = """CREATE TABLE IF NOT EXISTS readings (
                                                        device_id CHAR(50),
                                                        post_datetime CHAR(50),
                                                        post_date CHAR(50),
                                                        post_time CHAR(50),
                                                        voltage_l1_l12 NUMERIC (30,15),
                                                        voltage_l2_l23 NUMERIC (30,15),
                                                        voltage_l3_l31 NUMERIC (30,15),
                                                        current_l1 NUMERIC (30,15),
                                                        current_l2 NUMERIC (30,15),
                                                        current_l3 NUMERIC (30,15),
                                                        kw_l1 NUMERIC (30,15),
                                                        kw_l2 NUMERIC (30,15),
                                                        kw_l3 NUMERIC (30,15),
                                                        kvar_l1 NUMERIC (30,15),
                                                        kvar_l2 NUMERIC (30,15),
                                                        kvar_l3 NUMERIC (30,15),
                                                        kva_l1 NUMERIC (30,15),
                                                        kva_l2 NUMERIC (30,15),
                                                        kva_l3 NUMERIC (30,15),
                                                        power_factor_l1 NUMERIC (30,15),
                                                        power_factor_l2 NUMERIC (30,15),
                                                        power_factor_l3 NUMERIC (30,15),
                                                        total_kw NUMERIC (30,15),
                                                        total_kvar NUMERIC (30,15),
                                                        total_kva NUMERIC (30,15),
                                                        total_pf NUMERIC (30,15),
                                                        avg_frequency NUMERIC (30,15),
                                                        neutral_current NUMERIC (30,15),
                                                        volt_thd_l1_l12 NUMERIC (30,15),
                                                        volt_thd_l2_l23 NUMERIC (30,15),
                                                        volt_thd_l3_l31 NUMERIC (30,15),
                                                        current_thd_l1 NUMERIC (30,15),
                                                        current_thd_l2 NUMERIC (30,15),
                                                        current_thd_l3 NUMERIC (30,15),
                                                        current_tdd_l1 NUMERIC (30,15),
                                                        current_tdd_l2 NUMERIC (30,15),
                                                        current_tdd_l3 NUMERIC (30,15),
                                                        kwh_import NUMERIC (30,15),
                                                        kwh_export NUMERIC (30,15),
                                                        kvarh_import NUMERIC (30,15),
                                                        kvah_total NUMERIC (30,15),
                                                        max_amp_demand_l1 NUMERIC (30,15),
                                                        max_amp_demand_l2 NUMERIC (30,15),
                                                        max_amp_demand_l3 NUMERIC (30,15),
                                                        max_sliding_window_kw_demand NUMERIC (30,15),
                                                        accum_kw_demand NUMERIC (30,15),
                                                        max_sliding_window_kva_demand NUMERIC (30,15),
                                                        present_sliding_window_kw_demand NUMERIC (30,15),
                                                        present_sliding_window_kva_demand NUMERIC (30,15),
                                                        accum_kva_demand NUMERIC (30,15),
                                                        pf_import_at_maximum_kva_sliding_window_demand NUMERIC (30,15)
                                                        );
            """

def write_to_readings_table(**kwargs):
    
    sql = f"""INSERT INTO readings (
                                        device_id,
                                        post_datetime,
                                        post_date,
                                        post_time,
                                        voltage_l1_l12,
                                        voltage_l2_l23,
                                        voltage_l3_l31,
                                        current_l1,
                                        current_l2,
                                        current_l3,
                                        kw_l1,
                                        kw_l2,
                                        kw_l3,
                                        kvar_l1,
                                        kvar_l2,
                                        kvar_l3,
                                        kva_l1,
                                        kva_l2,
                                        kva_l3,
                                        power_factor_l1,
                                        power_factor_l2,
                                        power_factor_l3,
                                        total_kw,
                                        total_kvar,
                                        total_kva,
                                        total_pf,
                                        avg_frequency,
                                        neutral_current,
                                        volt_thd_l1_l12,
                                        volt_thd_l2_l23,
                                        volt_thd_l3_l31,
                                        current_thd_l1,
                                        current_thd_l2,
                                        current_thd_l3,
                                        current_tdd_l1,
                                        current_tdd_l2,
                                        current_tdd_l3,
                                        kwh_import,
                                        kwh_export,
                                        kvarh_import,
                                        kvah_total,
                                        max_amp_demand_l1,
                                        max_amp_demand_l2,
                                        max_amp_demand_l3,
                                        max_sliding_window_kw_demand,
                                        accum_kw_demand,
                                        max_sliding_window_kva_demand,
                                        present_sliding_window_kw_demand,
                                        present_sliding_window_kva_demand,
                                        accum_kva_demand,
                                        pf_import_at_maximum_kva_sliding_window_demand
                                        )
                                values(
                                        '{kwargs["device_id"]}',
                                        '{kwargs["post_datetime"]}',
                                        '{kwargs["post_date"]}',
                                        '{kwargs["post_time"]}',
                                        {kwargs["voltage_l1_l12"]},
                                        {kwargs["voltage_l2_l23"]},
                                        {kwargs["voltage_l3_l31"]},
                                        {kwargs["current_l1"]},
                                        {kwargs["current_l2"]},
                                        {kwargs["current_l3"]},
                                        {kwargs["kw_l1"]},
                                        {kwargs["kw_l2"]},
                                        {kwargs["kw_l3"]},
                                        {kwargs["kvar_l1"]},
                                        {kwargs["kvar_l2"]},
                                        {kwargs["kvar_l3"]},
                                        {kwargs["kva_l1"]},
                                        {kwargs["kva_l2"]},
                                        {kwargs["kva_l3"]},
                                        {kwargs["power_factor_l1"]},
                                        {kwargs["power_factor_l2"]},
                                        {kwargs["power_factor_l3"]},
                                        {kwargs["total_kw"]},
                                        {kwargs["total_kvar"]},
                                        {kwargs["total_kva"]},
                                        {kwargs["total_pf"]},
                                        {kwargs["avg_frequency"]},
                                        {kwargs["neutral_current"]},
                                        {kwargs["volt_thd_l1_l12"]},
                                        {kwargs["volt_thd_l2_l23"]},
                                        {kwargs["volt_thd_l3_l31"]},
                                        {kwargs["current_thd_l1"]},
                                        {kwargs["current_thd_l2"]},
                                        {kwargs["current_thd_l3"]},
                                        {kwargs["current_tdd_l1"]},
                                        {kwargs["current_tdd_l2"]},
                                        {kwargs["current_tdd_l3"]},
                                        {kwargs["kwh_import"]},
                                        {kwargs["kwh_export"]},
                                        {kwargs["kvarh_import"]},
                                        {kwargs["kvah_total"]},
                                        {kwargs["max_amp_demand_l1"]},
                                        {kwargs["max_amp_demand_l2"]},
                                        {kwargs["max_amp_demand_l3"]},
                                        {kwargs["max_sliding_window_kw_demand"]},
                                        {kwargs["accum_kw_demand"]},
                                        {kwargs["max_sliding_window_kva_demand"]},
                                        {kwargs["present_sliding_window_kw_demand"]},
                                        {kwargs["present_sliding_window_kva_demand"]},
                                        {kwargs["accum_kva_demand"]},
                                        {kwargs["pf_import_at_maximum_kva_sliding_window_demand"]}
                                )
                                                        ;
            """.format(kwargs)

    connect_write(sql)


def read_celery_results():

    data = connect_read("SELECT * FROM public.celery_taskmeta ORDER BY id ASC ")
    responses = []

    for response in data:

        real_data = pickle.loads(response[3], encoding='utf-8')
        responses.append(real_data)

    return responses


if __name__ == '__main__':
    create_readings_table()