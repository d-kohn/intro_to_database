#!/usr/bin/env python
# coding: utf-8

import psycopg2
import pandas as pd

def initialize():
    connection = psycopg2.connect(
        user = "XXXX", #username that you use
        password = "XXXX", #password that you use
        host = "XXXX", 
        port = "XXXX", 
        database = "XXXX"
    )
    connection.autocommit = True
    return connection

def runQuery(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        records = cursor.fetchall()
    return records

def displayQuery(records, headers):
    column_headers = []
    for header in headers:
        column_headers.append(header)
    results_df = pd.DataFrame(columns = column_headers)    
    for row in records:            
        results_df.loc[len(results_df.index)] = row    
    print(results_df)


def main():
    conn = initialize()
 
    # print("How many students in each school are SPED?")
    # query = "SELECT s.name, s.students, TO_CHAR(s.sped*100, 'fm99%') as ell, CAST(s.students * s.ell AS int) as ell_students FROM school s;"
    # results_headers = ['Name', 'Students', 'SPED % ', 'SPED Students']
    # records = runQuery(conn, query)
    # displayQuery(records, results_headers)
    # print()

    # print("Which High Schools have grades lower than 9th Grade and which grades do they have?\n")
    # query = "SELECT s.name, g.name FROM school s, grade_range g, schooltype st WHERE s.schooltype = st.type AND st.name='High School' AND 8 >= ANY(s.grade_level) AND g.grades = s.grade_level;"
    # results_headers = ['Name', 'Grade Range']
    # records = runQuery(conn, query)
    # displayQuery(records, results_headers)
    # print()

    # print("What are the names of principals who work in schools in Multnomah county and whose first\nor last name starts with ‘K’ and which school do they work at?\n")
    # query = "SELECT p.name, s.name as School FROM principal p, school s, county c WHERE s.county_id = c.id AND  c.name = 'Washington' AND p.id = s.principal_id AND p.name LIKE '%K%';"
    # results_headers = ['Name', 'School']
    # records = runQuery(conn, query)
    # displayQuery(records, results_headers)
    # print()

    print("What is the student to teacher ratio in the Clackamas county elementary schools?\n")
    query = "SELECT s.name, CAST(s.students / s.teachers AS int) FROM school s, county c WHERE s.county_id = c.id AND c.name = 'Clackamas' AND s.teachers > 0;"
    results_headers = ['Name', 'Students per Teacher']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)
    print()

if __name__ == "__main__":
    main()




