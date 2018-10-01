# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 16:13:34 2018

@author: ranajit
"""
import os
import sys
import copy
from __future__ import print_function
from collections import OrderedDict
sys.path.insert(0,os.getcwd() + "sqlparse")
import sqlparse
def fileExists(t):
    return os.path.isfile(t + '.csv')

def loadMetadata(loadMetaData_dict):
    m_file = open('metadata.txt', 'r');
    tab = False
    for content in m_file:
        if content.strip() == '<begin_table>':
            tab = True #Indicates the next line is a table name
            attr_names = []
            continue
        if tab:
            tab = False
            tab_name = content.strip()
            loadMetaData_dict[content.strip()] = []
            continue
        if content.strip() == '<end_table>':
            continue

        loadMetaData_dict[tab_name].append(content.strip())

    #Delete the tables from dictionary whose file doesn't exist
    for table in loadMetaData_dict:
        if fileExists(table) == False:
            del loadMetaData_dict[table]
            # print table + ".csv doesn't exist"

def extract_from_csv(table):
    table = table + '.csv'
    rw = []
    try:
        fp = open(table, 'r').readlines()
    except:
        raise NotImplementedError('Data cannot be loaded since table ' + tbl + ' does not exist')

    for r in fp:
        rw.append(r.rstrip("\r\n"))
    return rw

#Load meta data
metadata_dict = OrderedDict()
loadMetadata(metadata_dict)
GARBAGE_VAL = -9999999999
#Initialise the blank data structure
#{'table' : { col1 : [...], col2 : [...] ...}, 'table' : {......} ... }
db_ds = OrderedDict()
for tbl in metadata_dict:
    db_ds[tbl] = OrderedDict()
    for col in metadata_dict[tbl]:
        db_ds[tbl][col] = []

#Load table data into the data structure
for tbl in metadata_dict:
    db_rows = []
    db_rows = extract_from_csv(tbl)
    for row in db_rows:
        row = row.split(',')
        for i in range(len(row)):
            db_ds[tbl][metadata_dict[tbl][i]].append(int(row[i].strip('""')))

tbl_cnt = 0
def agg_func(func, tbl, col_name):

    if func == 'DISTINCT':
        tbl[col_name] = list(set(tbl[col_name]))
    elif func == 'MAX':
        mx = GARBAGE_VAL
        for value in tbl[col_name]:
            mx = max(int(value), mx)
        tbl[col_name] = [mx]
    elif func == 'MIN':
        mn = tbl[col_name][0]
        for value in tbl[col_name]:
            if value != GARBAGE_VAL:
                if mn > int(value):
                    mn = int(value)
        tbl[col_name] = [mn]
    elif func == 'COUNT':
        tbl[col_name] = [sum(1 for i in tbl[col_name] if i != GARBAGE_VAL)]
    elif func == 'SUM' or func == 'AVG':
        summ = sum(int(i) for i in tbl[col_name]  if i != GARBAGE_VAL)
        if func == 'AVG':
            l = sum(1 for i in tbl[col_name] if i != GARBAGE_VAL)
            try:
                tbl[col_name] = [summ / l]
            except Exception as e:
                tbl[col_name] = ['Infinity']
        else:
            tbl[col_name] = [summ]
    return tbl

# def handleConjunc(table1, table2, condition_clause):
#     table3 = copy.deepcopy(table1)
#     cond_cl_no = (len(condition_clause) - 2)/3
#     comp_clause_list = []
#     conjuction_list = []
#     x = 2
#     for i in range(cond_cl_no):
#         comp_clause_list.append(condition_clause[x])
#         x += 4
#     x = 4
#     for i in range(cond_cl_no - 1) :
#         conjuction_list.append(condition_clause[x])
#         x += 4
#     print (table1)
#     print ()
#     print (table2)
#     print ()
#     try:
#         for i in range(cond_cl_no):
#             if i > 0:
#                 col_to_be_compared = str(comp_clause_list[i].tokens[0])
#                 operator = str(comp_clause_list[i].tokens[2])
#                 print (col_to_be_compared)
#                 for j in range(len(table3[col_to_be_compared])):
#                     if not check_condition(int(table3[col_to_be_compared][j]), int(str(comp_clause_list[i].tokens[4])), operator):
#                         for col_title in table3:
#                             table3[col_title][j] = GARBAGE_VAL
#
#                 print ("table3")
#                 print(table3)
#                 print()
#                 table4 = copy.deepcopy(table1)
#                 if str(conjuction_list[i]).upper() == 'OR':
#                     for col, val in table2.iteritems():
#                         k = 0
#                         while k < len(table2[col]):
#                             if table3[col][k] == GARBAGE_VAL and table2[col][k] == GARBAGE_VAL:
#                                 table4[col][k] = GARBAGE_VAL
#                             k += 1
#                 elif str(conjuction_list[i]).upper() == 'AND':
#                     for col, val in table2.iteritems():
#                         k = 0
#                         while k < len(table2[col]):
#                             if int(table3[col][k]) == GARBAGE_VAL or int(table2[col][k]) == GARBAGE_VAL:
#                                 table4[col][k] = GARBAGE_VAL
#                             k += 1
#
#                 table3 = copy.deepcopy(table4)
#     except Exception as e:
#         pass
#
#     print (table3)
#     return table3

def distinct_values(tbl, col_to_be_printed):
    for i in col_to_be_printed:
        if '(' in i:
            raise Exception("Invalid syntax")
    table1 = OrderedDict()
    #Initialise blank table
    for cols in col_to_be_printed:
        table1[cols] = []
    temp_list = []
    for i in range(len(tbl[col_to_be_printed[0]])):
        k = 0
        temp_sub_list = []
        for k in col_to_be_printed:
            temp_sub_list.append(tbl[k][i])
        temp_list.append(temp_sub_list)
    temp_set = set(tuple(i) for i in temp_list)
    for vals in temp_set:
        for i in range(len(vals)):
            table1[col_to_be_printed[i]].append(vals[i])

    # print (table1)
    return table1

def showOutput(new_tbl, f_tbl, copied_tbl, conj, c_lst, distinct):
    #First make new_tbl as per the conditions
    if str(conj).upper() == 'OR':
        for col, val in f_tbl.iteritems():
            i = 0
            tbl_cnt = 0
            while i < len(f_tbl[col]):
                tbl_cnt += 1
                if copied_tbl[col][i] == GARBAGE_VAL and f_tbl[col][i] == GARBAGE_VAL:
                    new_tbl[col][i] = GARBAGE_VAL
                i += 1
    elif str(conj).upper() == 'AND':
        for col, val in f_tbl.iteritems():
            i = 0
            while i < len(f_tbl[col]):
                tbl_cnt = 0
                if int(copied_tbl[col][i]) == GARBAGE_VAL or int(f_tbl[col][i]) == GARBAGE_VAL:
                    new_tbl[col][i] = GARBAGE_VAL
                i += 1
    else:
        new_tbl = f_tbl

    duplicate_col = [] #To check if columns has been duplicated in case of join
    col_to_be_printed = [c.strip() for c in c_lst]



    if distinct:
        if c_lst[0] == "*":
            raise Exception("Query syntax error")
        else:
            new_tbl = distinct_values(new_tbl, col_to_be_printed)

    #Expand column names in case of *
    if c_lst[0] == "*" :
        col_to_be_printed = []
        for col, value in new_tbl.iteritems():
            col_name = (col.split('.'))[-1]
            if col_name not in duplicate_col:
                col_to_be_printed.append(col)
                duplicate_col.append(col_name)

    #Print Columnn names
    print_comma = 0
    for col in col_to_be_printed:
        if print_comma > 0:
            print ("," + str(col), end = '')
        else:
            print (str(col), end = '')
            print_comma += 1
    print('\r')

    for col in col_to_be_printed:
        if '(' in col:
            after_o_split = col.split('(')
            func = after_o_split[0]
            col = ((after_o_split[1]).split(')'))[0]
            new_tbl = agg_func(func.upper(), new_tbl, col)
            # col = (col_to_be_printed[0])
            col_to_be_printed = [((((col_to_be_printed[0]).split('('))[1]).split(')'))[0]]

    i = 0
    while i < len(new_tbl[col_to_be_printed[0]]):
        new_line_flag = True
        print_comma = 0
        for j in range(len(col_to_be_printed)):
            if new_tbl[col_to_be_printed[j]][i] != GARBAGE_VAL:
                new_line_flag = False
                if print_comma > 0:
                    print ("," + str(new_tbl[col_to_be_printed[j]][i]), end='')
                else:
                    print (str(new_tbl[col_to_be_printed[j]][i]), end='')
                    print_comma += 1
        i += 1
        if not new_line_flag:
            print()
        # break
    print()

def check_condition(a, b, operator):
    if operator == '=':
        return a == b
    elif operator == '<':
        return a < b
    elif operator == '<=':
        return a <= b
    elif operator == '>':
        return a > b
    elif operator == '>=':
        return a >= b
    else:
        raise NotImplementedError('operator ' + str(op) + ' is invalid')


def main():
    cmd_query = sys.argv[1]
    #Tokenise the query
    query = sqlparse.parse(cmd_query)[0]
    query_tokens = query.tokens
    distinct = False
    if(str(query_tokens[2]).upper() == 'DISTINCT'):
        distinct = True
        new_query = ""
        for i in cmd_query.split():
            if str(i).upper() != 'DISTINCT':
                new_query += i
                new_query += " "
        query = sqlparse.parse(new_query)[0]
        query_tokens = query.tokens
    if str(query_tokens[0]).upper() == 'SELECT':
        f_tbl = {}
        #Take out the column names and check if the name is not blank
        col_lst = [c.strip() for c in str(query_tokens[2]).split(',') if c]
        #Take out the table names and check if the name is not blank
        if str(query_tokens[4]).upper() == 'FROM':
            conj = None
            tab_lst = []
            for c in str(query_tokens[6]).strip().split(','):
                if c:
                    tab_lst.append(c.strip())

            #Check if the file exists in the DBMS
            for tbl in tab_lst:
                if tbl not in metadata_dict:
                    raise NameError('Table ' + tbl + ' does not exist')

            #Join multiple tables
            if len(tab_lst) >= 2:
                flag = False
                joined_table = {}
                #Append table name with column name in case of join
                i = 0
                tbl_cnt = 0
                while i in range(len(col_lst)):
                    for tbl in metadata_dict:
                        for c in metadata_dict[tbl]:
                            tbl_cnt += 1
                            if col_lst[i] == c:
                                col_lst[i] = str(tbl) + '.' + str(c)
                    i += 1
                #Duplicate column data in case of join
                tab_count = 1
                tab2_col = 0
                tab1_col = 0
                for col in metadata_dict[tab_lst[1]]:
                    tab2_col = len(db_ds[tab_lst[1]][col])
                    break
                for col in metadata_dict[tab_lst[0]]:
                    tab1_col = len(db_ds[tab_lst[0]][col])
                    break
                for tab in tab_lst:
                    for col in metadata_dict[tab]:
                        col_data_list = db_ds[tab][col]
                        if flag is False:
                            col_data_list = [j for j in col_data_list for i in range(tab2_col)]
                        else:
                            col_data_list = col_data_list * tab1_col
                        tab_count += 1;
                        joined_table[str(tab)+'.'+str(col)] = col_data_list
                    flag = True
                f_tbl = joined_table
                # print (f_tbl)
            else:
                temp_tab_lst = tab_lst[0]
                f_tbl = db_ds[temp_tab_lst]

            new_tbl = copy.deepcopy(f_tbl)
            copied_tbl = copy.deepcopy(f_tbl)
            #Where clause exists from 9th token
            if len(query.tokens) > 8:
                where_clause_tokens = query_tokens[8].tokens
                #check if first token is "where"
                if str(where_clause_tokens[0]).upper() == 'WHERE':
                    # print (where_clause_tokens)
                    comparison_token = where_clause_tokens[2]
                    w_col_name = str(comparison_token[0])
                    operator = str(comparison_token[2])
                    if len(tab_lst) > 1:
                        if '.' not in w_col_name:
                            for tab in tab_lst:
                                for col in db_ds[tab]:
                                    if w_col_name == col:
                                        w_col_name = tab + '.' + w_col_name
                    #Exception will occur for table1.A type of column name
                    try:
                        for i in range(len(f_tbl[w_col_name])):
                            if not check_condition(int(f_tbl[w_col_name][i]), int(str(comparison_token[4])), operator):
                                for col_title in f_tbl:
                                    f_tbl[col_title][i] = GARBAGE_VAL
                    except Exception as e:
                        tbl_cnt = 0
                        val = str(comparison_token[4])
                        if len(tab_lst) > 1:
                            if '.' not in val:
                                for tab in tab_lst:
                                    for col in db_ds[tab]:
                                        if val == col:
                                            val = tab + '.' + val
                        for i in range(len(f_tbl[w_col_name])):
                            if not check_condition(int(f_tbl[w_col_name][i]), int(f_tbl[val][i]), operator):
                                for col_title in f_tbl:
                                    tbl_cnt += 1
                                    f_tbl[col_title][i] = GARBAGE_VAL


                    try:
                        #Exception will occur if further conjuction clause is not in the query
                        #if and/or exists
                        conj = str(where_clause_tokens[4])
                        #what conditon should be checked
                        condition_tokens = where_clause_tokens[6]
                        col_to_be_compared = str(condition_tokens[0])
                        operator = str(condition_tokens[2])
                        try:
                            for i in range(len(copied_tbl[col_to_be_compared])):
                                if not check_condition(int(copied_tbl[col_to_be_compared][i]), int(str(condition_tokens[4])), operator):
                                    for col_title in copied_tbl:
                                        copied_tbl[col_title][i] = GARBAGE_VAL
                        except Exception as e:
                            val = str(condition_tokens[4])
                            for i in range(len(copied_tbl[col_to_be_compared])):
                                if not check_condition(int(copied_tbl[col_to_be_compared][i]), int(copied_tbl[val][i]), operator):
                                    tbl_tbl_cnt = 0
                                    for col_title in copied_tbl:
                                        copied_tbl[col_title][i] = GARBAGE_VAL

                    except Exception as e:
                        conj = None
            # print (f_tbl)
            # print (copied_tbl)
            # print (new_tbl)
            #Print the output
            # handleConjunc(new_tbl, f_tbl, where_clause_tokens)
            showOutput(new_tbl, f_tbl, copied_tbl, conj, col_lst, distinct)
        else:
            raise Exception('Syntax is not valid')
    else:
        print ("Only SELECT is supported")

if __name__ == "__main__":
    main()
