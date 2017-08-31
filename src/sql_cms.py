import re
import psycopg2
from psycopg2 import extras
import json
from exceptions import Exception, AssertionError

user = "adlentz"

dbname='alysonlentz'
user='alysonlentz'
password=''


def cursor_connect(dbname, user, password, cursor_factory=None):
    """
    Connects to the DB and returns the connection and cursor, ready to use.

    Parameters
    ----------
    db_dsn : str, unicode
        DSN of the database to connect to.
    cursor_factory : psycopg2.extras
        An optional psycopg2 cursor type, e.g. DictCursor.

    Returns
    -------
    (psycopg2.extensions.connection, psycopg2.extensions.cursor)
        A tuple of (psycopg2 connection, psycopg2 cursor).
    """
    con = psycopg2.connect(dbname=dbname, user=user, host='localhost', port=5432, password=password)
    if not cursor_factory:
        cur = con.cursor()
    else:
        cur = con.cursor(cursor_factory=cursor_factory)
    return con, cur


def disease_age_death(col):
    """
    Get the states in descending order of the average age of death,
    where disease corresponds to the column name.

    Parameters
    ----------
    col : str, unicode
        A column name.

    Returns
    -------
    json
        A labeled JSON object with the state and average age of death for
        that state when the disease entered is true.
    """
    disease = []
    accepted_cols = (
        "alz_rel_sen",
        "heart_fail",
        "chronic_kidney",
        "cancer",
        "depression"
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(dbname, user, password, psycopg2.extras.DictCursor)
        query = """
        SELECT state, AVG(FLOOR(CAST(SUBSTR(dod, 1, 4) AS int) - CAST(SUBSTR(dob, 1, 4) AS int))) AS avg_age_death 
        FROM {0}
        WHERE {1}='true' AND dod IS NOT NULL
        GROUP BY state
        ORDER BY avg_age_death DESC;""".format("cmspop", cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            age_death = {row['state']: row['avg_age_death']}
            disease.append(age_death)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return json.dumps(disease)


def disease_reimb_resp(col):
    """
    Get the states average carrier reimbursement for that disease and
    in descending order of the average beneficiary responsibility,
    where disease corresponds to the column name.

    Parameters
    ----------
    col : str, unicode
        A column name.

    Returns
    -------
    json
        A labeled JSON object with the state and average beneficiary responsibility and
        percentage of carrier reimbursement for the selected disease within that state.
    """
    disease = []
    accepted_cols = (
        "alz_rel_sen",
        "heart_fail",
        "chronic_kidney",
        "cancer",
        "depression"
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(dbname, user, password, psycopg2.extras.DictCursor)
        query = """
        SELECT state, AVG(RHS.carrier_reimb) AS avg_carrier_reimb, AVG(RHS.bene_resp) AS avg_bene_resp 
        FROM (SELECT id, state FROM {0} WHERE {2} = True) AS LHS 
        LEFT JOIN (SELECT id, carrier_reimb, bene_resp FROM {1}) 
        AS RHS ON LHS.id = RHS.id
        GROUP BY state 
        ORDER BY avg_bene_resp DESC;""".format("cmspop", "cmsclaims", cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            reimb_resp = {row['state']: {'avg_carrier_reimb': float(row['avg_carrier_reimb']),
                                         'average_bene_resp': float(row['avg_bene_resp'])}}
            disease.append(reimb_resp)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return json.dumps(disease)


def disease_ratio_sex(col):
    """
    Get the states in descending order of the ratio of male to female patients,
    where disease corresponds to the column name.

    Parameters
    ----------
    col : str, unicode
        A column name.

    Returns
    -------
    json
        A labeled JSON object with the state and ratio of male to female
        patients for each state corresponding to the disease entered.
    """
    disease = []
    accepted_cols = (
        "alz_rel_sen",
        "heart_fail",
        "chronic_kidney",
        "cancer",
        "depression"
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(dbname, user, password, psycopg2.extras.DictCursor)
        query = """
        SELECT state, male/female::float AS male_female_ratio FROM (SELECT
        LHS.state AS state, male, female FROM (SELECT state, count(*) AS
        male FROM {0} WHERE {1}='true' AND sex='male' GROUP BY state)
        AS LHS LEFT JOIN (SELECT state, count(*) AS female FROM
        {0} WHERE {1}='true' AND sex='female' GROUP BY state) AS RHS
        ON LHS.state=RHS.state) AS outer_q
        ORDER by male_female_ratio DESC;""".format("cmspop", cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            ratio = {row['state']: float(row['male_female_ratio'])}
            disease.append(ratio)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return json.dumps(disease)


def disease_reimb_race(col):
    """
    Get the race and average inpatient reimbursement and average outpatient reimbursement
    for the disease that corresponds to the column name and the state selected.

    Parameters
    ----------
    col : str, unicode
        A column name.

    state_abbr: str, unicode
        A selection from the state column.

    Returns
    -------
    json
        A labeled JSON object with the race and average inpatient and outpatient
        reimbursement amount for the selected disease and state.
    """
    disease = []
    accepted_cols = (
        "alz_rel_sen",
        "heart_fail",
        "cancer",
        "depression"
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(dbname, user, password, psycopg2.extras.DictCursor)
        query = """
        SELECT state, race,
        ((total_reimb / aggregate_carrier_reimb::float) * 100)::float AS percent_of_reimbs,
        ((total_deaths / aggregate_deaths::float) * 100)::float AS percent_of_deaths
        FROM (SELECT LLHS.state, LLHS.race, LLHS.total_carrier_reimb AS total_reimb, 
        LLHS.total_deaths_race AS total_deaths, RRHS.aggregate_carrier_reimb, RRHS.aggregate_deaths FROM
        (SELECT LHS.state, LHS.race, SUM(carrier_reimb) AS total_carrier_reimb,
        COUNT(*) AS total_deaths_race FROM (SELECT id, state, race FROM {0}
        WHERE {2} = True and dod IS NOT NULL) AS LHS LEFT JOIN (SELECT id, carrier_reimb FROM {1}) AS RHS
        ON LHS.id=RHS.id GROUP BY state, race) AS LLHS LEFT JOIN (SELECT state, 
        SUM(carrier_reimb) AS aggregate_carrier_reimb, COUNT(*) AS aggregate_deaths FROM
        (SELECT id, state, race FROM {0} WHERE {2} = True and dod IS NOT NULL) AS LHS
        LEFT JOIN (SELECT id, carrier_reimb FROM {1}) AS RHS ON LHS.id=RHS.id
        GROUP BY state) AS RRHS ON LLHS.state = RRHS.state) AS sub_q
        ORDER BY state ASC;""".format("cmspop", "cmsclaims", cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            reimb = {row['state']: {'race': row['race'], 
                                    'percentage_deaths': row['percent_of_deaths'],
                                    'percentage_reimbursement': row['percent_of_reimbs']}}
            disease.append(reimb)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return json.dumps(disease)


def disease_death_numbers(col):
    """
    Get the states in descending order of the total number of deaths,
    youngest age at death and oldest age at death,
    where disease corresponds to the column name.

    Parameters
    ----------
    col : str, unicode
        A column name.

    Returns
    -------
    json
        A labeled JSON object with the state and total number of deaths,
        youngest age at death, and oldest age at death for the
        selected disease in each state.
    """
    disease = []
    accepted_cols = (
        "alz_rel_sen",
        "heart_fail",
        "chronic_kidney",
        "cancer",
        "depression"
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            raise AssertionError("Column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(dbname, user, password, psycopg2.extras.DictCursor)
        query = """
        SELECT state, disease_deaths, MIN(age_of_death) AS youngest_death, MAX(age_of_death) AS oldest_death
        FROM (SELECT LHS.state AS state, disease_deaths, age_of_death FROM (SELECT state, 
        FLOOR(CAST(SUBSTR(dod, 1, 4) AS int) - CAST(SUBSTR(dob, 1, 4) AS int)) AS age_of_death 
        FROM {0} WHERE {1}='true' AND dod IS NOT NULL) AS LHS LEFT JOIN (SELECT state, COUNT(*)
        AS disease_deaths FROM {0} WHERE {1}='true' AND dod IS NOT NULL GROUP BY state) AS RHS ON LHS.state=RHS.state)
        AS outer_q GROUP BY state, disease_deaths ORDER by disease_deaths DESC;""".format("cmspop", cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            age = {row['state']: {'total_deaths': row['disease_deaths'],
                                  'youngest_death': row['youngest_death'],
                                  'oldest_death': row['oldest_death']}}
            disease.append(age)
    except Exception as e:
        raise Exception("Error: {}".format(e.message))
    return json.dumps(disease)

if __name__ == '__main__':
    print "States in descending order of the average age of death for Alzheimer's Disease:"
    print disease_age_death("alz_rel_sen")
    
    print "States in descending order of the average beneficiary responsibility, including the average carrier reimbursement for cancer: "
    print disease_reimb_resp("cancer")
    
    print "States in descending order of the ratio of male to female patients for depression: "
    print disease_ratio_sex("depression")
    
    print "Percentage of total reimbursement and percentage of total deaths by race, grouped by state for cancer: "
    print disease_reimb_race("cancer")
    
    print "States in descending order of the total number of deaths, youngest age at death, and oldest age at death for heart failure: "
    print disease_death_numbers("heart_fail")
    
    