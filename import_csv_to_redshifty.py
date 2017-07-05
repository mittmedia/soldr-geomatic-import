#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
reload(sys)
sys.setdefaultencoding('utf8')
import csv
import psycopg2

column_map = {
    "Gatunamn" : { "name" : "street_name", "type" : "VARCHAR(512)" },
    "Gatunummer" : { "name" : "street_number", "type" : "VARCHAR(512)" },
    "Postnummer" : { "name" : "zipcode", "type" : "VARCHAR(512)" },
    "Ålder" : { "name" : "age", "type" : "VARCHAR(512)" },
    "FM_Villa" : { "name" : "fm_villa", "type" : "BOOLEAN" },
    "FM_Villa_Prob" : { "name" : "fm_villa_prob", "type" : "FLOAT" },
    "FM_Age_HasChildren" : { "name" : "fm_age_haschildren", "type" : "BOOLEAN" },
    "FM_Age_Child_Prob" : { "name" : "fm_age_child_prob", "type" : "FLOAT" },
    "FM_NoAge_HasChildren" : { "name" : "fm_noage_haschildren", "type" : "BOOLEAN" },
    "FM_NoAge_Child_Prob" : { "name" : "fm_noage_child_prob", "type" : "FLOAT" },
    "FM_Age_IncomeLevel" : { "name" : "fm_age_incomelevel", "type" : "SMALLINT" },
    "FM_NoAge_IncomeLevel" : { "name" : "fm_noage_incomelevel", "type" : "SMALLINT" },
    "SM_Villa" : { "name" : "sm_villa", "type" : "BOOLEAN" },
    "SM_Villa_Prob" : { "name" : "sm_villa_prob", "type" : "FLOAT" },
    "SM_Age_HasChildren" : { "name" : "sm_age_haschildren", "type" : "BOOLEAN" },
    "SM_Age_Child_Prob" : { "name" : "sm_age_child_prob", "type" : "FLOAT" },
    "SM_NoAge_HasChildren" : { "name" : "sm_noage_haschildren", "type" : "BOOLEAN" },
    "SM_NoAge_Child_Prob" : { "name" : "sm_noage_child_prob", "type" : "FLOAT" },
    "SM_Age_IncomeLevel" : { "name" : "sm_age_incomelevel", "type" : "SMALLINT" },
    "SM_NoAge_IncomeLevel" : { "name" : "sm_noage_incomelevel", "type" : "SMALLINT" },
    "PM_Villa_Prob" : { "name" : "pm_villa_prob", "type" : "FLOAT" },
    "PM_Age_HasChildren" : { "name" : "pm_age_haschildren", "type" : "BOOLEAN" },
    "PM_Age_Child_Prob" : { "name" : "pm_age_child_prob", "type" : "FLOAT" },
    "PM_NoAge_HasChildren" : { "name" : "pm_noage_haschildren", "type" : "BOOLEAN" },
    "PM_NoAge_Child_Prob" : { "name" : "pm_noage_child_prob", "type" : "FLOAT" },
    "PM_Age_IncomeLevel" : { "name" : "pm_age_incomelevel", "type" : "SMALLINT" },
    "PM_NoAge_IncomeLevel" : { "name" : "pm_noage_incomelevel", "type" : "SMALLINT" }
}

yes_no = {
    "Yes": True,
    "No": False
}

level = {
    "Low": 0,
    "Medium": 5,
    "High": 10
}

def findIndex(header_name, headers):
    for i in range(len(headers)):
        if headers[i] == header_name:
            return i
    return -1

def create_table(headers_list):
    create_table_command = "CREATE TABLE IF NOT EXISTS geomatic_address (\n"
    create_table_command = create_table_command + "id BIGINT identity(0,1),\n"
    create_table_command = create_table_command + "street_address VARCHAR(512),\n"
    for column_key in headers_list:
        column = column_map[column_key]
        create_table_command = create_table_command + column["name"] + " " + column["type"] + ",\n"
    create_table_command = create_table_command[:-2]
    create_table_command = create_table_command + "\n);"
    return create_table_command

def insert_query(headers_list, adr):
    records_list_template = ','.join(['%s'] * len(adr))
    column_names = []
    column_names.append("street_address")
    for column_key in headers_list:
        column_names.append(column_map[column_key]["name"])
    return 'insert into geomatic_address ({0}) values {1}'.format(
        ','.join(column_names) ,records_list_template)


def convert_value(column_name, value):
    if value == '':
        return None

    column_type = column_map[column_name]["type"]

    if column_type == "BOOLEAN":
        #print column_name
        #print value
        return yes_no[value]
    elif column_type == "SMALLINT":
        return level[value]

    return value

conn = psycopg2.connect(host=os.environ['REDSHIFT_URL'], port=5439, database='dev', user=os.environ['REDSHIFT_USER'], password=os.environ['REDSHIFT_PASSWORD'])
cur = conn.cursor()


addresses = []
f = open('geo_import_data.csv', 'r')
lines = f.read().split("\n")

header = lines[0].replace('\r', '')
dataframe = [row for row in lines[1:]]
h = header.split(';')
cur.execute("DROP TABLE IF EXISTS geomatic_address;")
cur.execute(create_table(h))
for row in dataframe:
    row = row.replace('\r', '').replace('\"', '')
    if row != "":
        row_values = row.split(';')

        mapOfValues = map( lambda (i,x): convert_value(h[i],x), enumerate(row_values))
        mapOfValues.insert(0,row_values[findIndex("Gatunamn",h)].rstrip() + " " + row_values[findIndex("Gatunummer",h)].rstrip())
        addresses.append(tuple(mapOfValues))
insert_query = insert_query(h, addresses)
print addresses[:1]
print insert_query
cur.execute(insert_query, addresses)

conn.commit()
cur.close()
conn.close()






















