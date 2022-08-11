#!/usr/bin/env python
# coding: utf-8

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def main():
    conn = initialize() 
#    question_1(conn)
#    question_2(conn)
#    question_3(conn)
#    question_4(conn)
#    question_5(conn)
#    question_6(conn)
#    question_7(conn)
    question_8(conn)
#    question_9(conn)
#    question_10(conn)

def question_1(conn):
    print("What are the average on track, on-time, five-year graduation rates by district?\n")
    query = "SELECT d.name, TO_CHAR(AVG(g.on_track)*100, 'fm99%'), TO_CHAR(AVG(g.on_time_grad)*100, 'fm99%'), TO_CHAR(AVG(g.five_year_grad)*100, 'fm99%') FROM school s, district d, graddata g WHERE s.district_id = d.id AND g.school_id = s.id GROUP BY d.name;"
    results_headers = ['District', 'On Track to Graduation', 'On-time Graduation', 'Five-year Graduation']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)
    # Visualization
    query = "SELECT d.name, AVG(g.on_track), AVG(g.on_time_grad), AVG(g.five_year_grad) FROM school s, district d, graddata g WHERE s.district_id = d.id AND g.school_id = s.id GROUP BY d.name"
    records = runQuery(conn, query)
    labels = ['School District', 'Graduation Rates', "On-time and 5-year Graduation rates by school district"]
    legend = ['On-track', 'On-time', '5-year']    
    y_labels = percent_label(step=5)
    visualizeQueryBargraph(records, results_headers, labels, legend, y_labels)

def question_2(conn):
    print("What is the student to teacher ratio in the Clackamas county elementary schools?\n")
    query = "SELECT s.name, CAST(s.students / s.teachers AS int) FROM school s, county c WHERE s.county_id = c.id AND c.name = 'Clackamas' AND s.teachers > 0 AND s.schooltype = (SELECT st.type FROM schooltype st WHERE st.name = 'Elementary');"
    results_headers = ['Name', 'Students per Teacher']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)
    # Visualization
    records = runQuery(conn, query)
    labels = ['School', 'Students per Teacher Ratio', "Student to teacher ratio by school in Clackamas county"]
    visualizeQueryBargraph(records, results_headers, labels)

def question_3(conn):
    print("How many students are in each high school and how many are in SPED?")
    query = "SELECT s.name, s.students, TO_CHAR(s.sped*100, 'fm99%') as sped, CAST(s.students * s.sped AS int) as sped_students FROM school s, district d WHERE s.district_id = d.id AND d.name = 'Portland SD 1J' AND s.schooltype = (SELECT st.type FROM schooltype st WHERE st.name = 'High School');"
    results_headers = ['Name', 'Students', 'SPED % ', 'SPED Students']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)
    # Visualization
    query = "SELECT s.name, s.students, CAST(s.students * s.sped AS int) FROM school s, district d WHERE s.district_id = d.id AND d.name = 'Portland SD 1J' AND s.schooltype = (SELECT st.type FROM schooltype st WHERE st.name = 'High School');"
    results_headers = ['Name', 'Students', 'SPED Students']
    records = runQuery(conn, query)
    labels = ['School', 'Students and SPED Students', "SPED students and total students in Portland high schools"]
    legend = ['Total students', 'SPED students']    
    visualizeQueryBargraph(records, results_headers, labels, legend)


def question_4(conn):
    print("Which High Schools have grades lower than 9th Grade and which grades do they have?\n")
    query = "SELECT s.name, g.name FROM school s, grade_range g, schooltype st WHERE s.schooltype = st.type AND st.name='High School' AND 8 >= ANY(s.grade_level) AND g.grades = s.grade_level;"
    results_headers = ['Name', 'Grade Range']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)

def question_5(conn):
    print("What are the names of principals who work in schools in Multnomah county and whose first\nor last name starts with ‘K’ and which school do they work at?\n")
    query = "SELECT p.name, s.name as School FROM principal p, school s, county c WHERE s.county_id = c.id AND  c.name = 'Washington' AND p.id = s.principal_id AND p.name LIKE '%K%';"
    results_headers = ['Name', 'School']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)

def question_6(conn):
    print("How many principals have been in their position for three or more years?\n")
    query = "SELECT COUNT(p.id) FROM principal p WHERE p.three_years = true;"
    results_headers = ['Count']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)

def question_7(conn):
    print("What 10 high schools have the highest college bound student rates, listed with their district, principal, and county\n")
    query = "SELECT s.name, p.name, d.name, c.name, TO_CHAR(gd.college_bound*100, 'fm999%') FROM graddata gd, principal p, school s, district d, county c, districtcountyrel dcr WHERE gd.college_bound IS NOT NULL AND s.id = gd.school_id AND s.district_id = d.id AND p.id = s.principal_id AND d.id = dcr.district_id AND dcr.county_id = c.id ORDER BY gd.college_bound DESC LIMIT 10;"
    results_headers = ['School','Principal', 'District', 'County', 'College Bound Students']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)

def question_8(conn):
    print("What are the 5 smallest school districts in Clackamas county?\n")
    query = "SELECT d.name, SUM(ss.students) FROM (SELECT s.district_id, s.students FROM school s, district d WHERE s.district_id = d.id) AS ss, district d where ss.district_id = d.id GROUP BY d.name order by SUM(ss.students) LIMIT 5"
    results_headers = ['District','Students']
    records = runQuery(conn, query)
    displayQuery(records, results_headers)
    

def question_9(conn):
    pass

def question_10(conn):
    pass




def initialize():
    connection = psycopg2.connect(
        user = "read_user", #username that you use
        password = "postgrespassword!", #password that you use
        host = "35.233.150.47", 
        port = "5432", 
        database = "postgres"
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

def visualizeQueryBargraph(records, headers, labels, legend = None, y_labels = None):
    X_LABEL = 0
    Y_LABEL = 1
    TITLE = 2
    COLORS = {0 : 'red', 1 : 'blue', 2 : 'green', 3 : 'purple', 4 : 'orange'}

    results_df = pd.DataFrame(data = records, columns = headers)    
    plt.style.use('ggplot')
    rows = len(results_df)
    fig = plt.figure(figsize = (rows/2, 6))

    x_pos = [i for i, _ in enumerate(list(results_df[headers[0]]))]
    
    X = np.arange(len(results_df[headers[0]]))
    data_columns = results_df.shape[1]-1
    if (data_columns > 1):
        col_width = 0.8/data_columns
        offset = ((data_columns) * col_width) / 4
        for i in range(0, data_columns):
            plt.bar(X-offset+(i*col_width), list(results_df[headers[i+1]]), color = COLORS[i], width = col_width)
    else:
        plt.bar(X, list(results_df[headers[1]]), color = 'red', width = 0.5)


    plt.xlabel(labels[X_LABEL])
    plt.ylabel(labels[Y_LABEL])
    plt.title(labels[TITLE])
    plt.xticks(x_pos, list(results_df[headers[0]]), rotation=90)
    if (y_labels != None):
        y_pos = [i*0.05 for i, _ in enumerate(y_labels)]
        plt.yticks(y_pos, y_labels)
    if (legend != None):
        plt.legend(legend, ncol = data_columns, fontsize=7)
    fig.tight_layout()
    plt.show()

def percent_label(step=10):
    labels = []
    for i in range(0, 101, step):
        labels.append(f'{i}%')
    return labels


if __name__ == "__main__":
    main()




