'''
Automatisation de la création de la base de données PostgreSQL pour Health Insurance Claims System
'''

# libraries
import psycopg2
import os
import json
import csv
from datetime import datetime
import pandas as pd
import numpy as np

dt_add= pd.read_csv('Datasets/add.csv')
dt_claim= pd.read_csv('Datasets/claim.csv')
dt_claim_p= pd.read_csv('Datasets/claim_payment.csv')
dt_cov= pd.read_csv('Datasets/cov.csv')
dt_mem= pd.read_csv('Datasets/mem.csv')
dt_pr= pd.read_csv('Datasets/pr.csv')
dt_st= pd.read_csv('Datasets/st.csv')

# Pré- traitement des tables
# dt_add ------ ok 
# dt_claim
dt_claim["date_of_service"] = pd.to_datetime(dt_claim["date_of_service"], format='%b %d, %Y')
dt_claim["received_date"] = pd.to_datetime(dt_claim["received_date"], format='%b %d, %Y')

dt_claim['day_of_service'] = dt_claim['date_of_service'].dt.day
dt_claim['month_of_service'] = dt_claim['date_of_service'].dt.month
dt_claim['year_of_service'] = dt_claim['date_of_service'].dt.year

dt_claim['received_day'] = dt_claim['received_date'].dt.day
dt_claim['received_month'] = dt_claim['received_date'].dt.month
dt_claim['received_year'] = dt_claim['received_date'].dt.year

dt_claim.to_csv('Datasets/claim_vf.csv', index=False)

# dt_claim_payment --------- ok 
# dt_cov
# Remplacement des valeurs "NULL" par NaN
dt_cov.replace('NULL', np.nan, inplace=True)


dt_cov["effective_date"] = pd.to_datetime(dt_cov["effective_date"], format="%d-%b-%y", errors='coerce')
dt_cov["term_date"] = pd.to_datetime(dt_cov["term_date"], format="%m/%d/%y", errors='coerce')

dt_cov['day_effective'] = dt_cov['effective_date'].dt.day
dt_cov['month_effective'] = dt_cov['effective_date'].dt.month
dt_cov['year_effective'] = dt_cov['effective_date'].dt.year

dt_cov['term_day'] = dt_cov['term_date'].dt.day
dt_cov['term_month'] = dt_cov['term_date'].dt.month
dt_cov['term_year'] = dt_cov['term_date'].dt.year

dt_cov.to_csv('Datasets/cov_vf.csv', index=False)

# dt_mem
dt_mem["member_dob"] = pd.to_datetime(dt_mem["member_dob"], format="%d-%m-%Y")

dt_mem['day_memberD'] = dt_mem['member_dob'].dt.day
dt_mem['month_memberD'] = dt_mem['member_dob'].dt.month
dt_mem['year_memberD'] = dt_mem['member_dob'].dt.year

dt_mem.to_csv('Datasets/mem_vf.csv', index=False)

# dt_pr ---------- ok
# dt_st ---------- ok


# Charger les informations sensibles depuis le fichier JSON
with open('/Health Insurance/Scripts/config_file.json', 'r') as file:
    config = json.load(file)

# Extraire les informations de connexion
dbname = config.get('dbname')
user = config.get('user')
password = config.get('password')
host = config.get('host')

# Se connecter à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host
)
cur = conn.cursor()

# Create the schema
cur.execute('''
    CREATE SCHEMA IF NOT EXISTS insurance;
''')


# *1* Create the table address within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.address (
        address_id INT PRIMARY KEY NOT NULL,
        street_address VARCHAR(70),
        apartment_no INT NOT NULL,
        city VARCHAR(70),
        county VARCHAR(70),
        country VARCHAR(70),
        zipcode VARCHAR(70)
    );
''')

# *2* Create the table coverage within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.coverage (
        coverage_id INT PRIMARY KEY NOT NULL,
        member_id INT NOT NULL,
        coverage_name VARCHAR(75),
        effective_date VARCHAR(75),
        day_effective VARCHAR(75),
	    month_effective VARCHAR(75),
	    year_effective VARCHAR(75),
        term_date VARCHAR(75),
	    term_day VARCHAR(75),
        term_month VARCHAR(75),
        term_year VARCHAR(75)
    );
''')

# *3* Create the table status within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.status (
        status_id INT PRIMARY KEY NOT NULL,
        claim_status VARCHAR(75),
        type VARCHAR(75)      
    );
''')

# *4* Create the table claims within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.claims (
        claim_id INT PRIMARY KEY NOT NULL,
        status_id INT NOT NULL,
        date_of_service DATE,
        day_of_service INT,
        month_of_service INT,
        year_of_service INT,
        received_date DATE,
        received_day INT,
        received_month INT,
        received_year INT,
	    add_by INT
    );
''')

# *5* Create the table claims_payment within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.claims_payment (
        claims_payment_id INT PRIMARY KEY NOT NULL,
        billed_amount INT,
        approuved_amount INT,
        copay_amount INT,
        coinsurance_amount INT,
        deductible_amount INT,
        net_payment INT,
        claim_id INT NOT NULL,
        FOREIGN KEY (claim_id) REFERENCES insurance.claims(claim_id)
        
    );
''')

# *6* Create the table provider within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.provider (
        provider_id INT PRIMARY KEY NOT NULL,
        provider_first_name VARCHAR(75),
        provider_last_name VARCHAR(75),
        degree VARCHAR(75),
        network VARCHAR(75),
        claim_id INT NOT NULL,
        practice_name VARCHAR(75),
        address_id INT,
        gender VARCHAR(75),
        FOREIGN KEY (address_id) REFERENCES insurance.address(address_id),
        FOREIGN KEY (claim_id) REFERENCES insurance.claims(claim_id)
        
    );
''')

# *7* Create the table member within the schema
cur.execute('''
    CREATE TABLE IF NOT EXISTS insurance.member (
        member_id INT PRIMARY KEY NOT NULL,
        member_first_name VARCHAR(75),
        member_last_name VARCHAR(75),
        address_id INT NOT NULL,
        gender VARCHAR(75),
        member_dob DATE,
        day_memberD INT,
        month_memberD INT,
        year_memberD INT,
        claim_id INT NOT NULL,
        coverage_id INT NOT NULL,
        FOREIGN KEY (address_id) REFERENCES insurance.address(address_id),
        FOREIGN KEY (claim_id) REFERENCES insurance.claims(claim_id),
        FOREIGN KEY (coverage_id) REFERENCES insurance.coverage(coverage_id)
        
    );
''')

# Import add data from the CSV file
with open('Datasets/add.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        address_id = int(row['address_id'])
        apartment_no = int(row['apartment_no'])
        
        # Insérer ou mettre à jour les données en cas de conflit
        cur.execute("""
        INSERT INTO insurance.address (address_id, street_address, apartment_no, city, county, country, zipcode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (address_id) DO UPDATE
        SET address_id = EXCLUDED.address_id,
        street_address = EXCLUDED.street_address,
        apartment_no = EXCLUDED.apartment_no,
        city = EXCLUDED.city,
        county = EXCLUDED.county,
        country = EXCLUDED.country,
        zipcode = EXCLUDED.zipcode;
        """, (address_id, row['street_address'], apartment_no, row['city'], row['county'], row['country'], row['zipcode']))

        # Import claim data from the CSV file
with open('Datasets/claim_vf.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        claim_id = int(row['claim_id'])
        status_id = int(row['status_id'])
        add_by = int(row['add_by'])

        # Insérer ou mettre à jour les données en cas de conflit
        cur.execute("""
        INSERT INTO insurance.claims (claim_id, status_id, date_of_service, received_date, add_by, 
                    day_of_service, month_of_service, year_of_service, received_day,  received_month,  received_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (claim_id) DO UPDATE
        SET status_id = EXCLUDED.status_id,
        date_of_service = EXCLUDED.date_of_service,
        received_date = EXCLUDED.received_date,
        add_by = EXCLUDED.add_by;
        """, (claim_id, status_id, row["date_of_service"], row["received_date"], add_by, 
              row["day_of_service"], row["month_of_service"], row["year_of_service"], row["received_day"],
                  row["received_month"],  row["received_year"]))
        

        # Import coverage data from csv file
# Importer les données de couverture
with open('Datasets/cov_vf.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        coverage_id = int(row['coverage_id'])
        member_id = int(row['member_id'])
        effective_date = row['effective_date']  
        term_date = row['term_date']  

        cur.execute("""
        INSERT INTO insurance.coverage (coverage_id, member_id, coverage_name, effective_date, 
                    day_effective, month_effective, year_effective, term_date, term_day, term_month, term_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (coverage_id) DO UPDATE
        SET member_id = EXCLUDED.member_id,
            coverage_name = EXCLUDED.coverage_name,
            effective_date = EXCLUDED.effective_date,
            day_effective = EXCLUDED.day_effective,
            month_effective = EXCLUDED.month_effective,
            year_effective = EXCLUDED.year_effective,
            term_date = EXCLUDED.term_date,
            term_day = EXCLUDED.term_day,
            term_month = EXCLUDED.term_month,
            term_year = EXCLUDED.term_year;
        """, (coverage_id, member_id, row['coverage_name'], effective_date, row['day_effective'], 
              row['month_effective'], row['year_effective'], term_date, row['term_day'], 
              row['term_month'], row['term_year']))


# Import claim_payment data from csv file
with open('Datasets/claim_payment.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        claims_payment_id = int(row['claim_payment_id'])
        billed_amount = int(row['billed_amount'])
        approuved_amount = int(row['approved_amount'])
        copay_amount = int(row['copay_amount'])
        coinsurance_amount = int(row['coinsurance_amount'])
        deductible_amount = int(row['deductible_amount'])
        net_payment = int(row['net_payment'])
        claim_id = int(row['claim_id'])

        cur.execute("""
        INSERT INTO insurance.claims_payment (claims_payment_id, billed_amount, approuved_amount, copay_amount, 
                    coinsurance_amount, deductible_amount, net_payment, claim_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (claims_payment_id) DO UPDATE
        SET billed_amount = EXCLUDED.billed_amount,
            approuved_amount = EXCLUDED.approuved_amount,
            copay_amount = EXCLUDED.copay_amount,
            coinsurance_amount = EXCLUDED.coinsurance_amount,
            deductible_amount = EXCLUDED.deductible_amount,
            net_payment = EXCLUDED.net_payment,
            claim_id = EXCLUDED.claim_id;
        """, (claims_payment_id, billed_amount, approuved_amount, copay_amount, coinsurance_amount, 
              deductible_amount, net_payment, claim_id))


# Import provider data from csv file
with open('Datasets/pr.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        provider_id = int(row['provider_id'])
        provider_first_name = row['provider_first_name']
        provider_last_name = row['provider_last_name']
        degree = row['degree']
        network = row['network']
        claim_id = int(row['claim_id'])
        practice_name = row['practice_name']
        address_id = int(row['address_id'])
        gender = row['gender']

        cur.execute("""
        INSERT INTO insurance.provider (provider_id, provider_first_name, provider_last_name, degree, 
                    network, claim_id, practice_name, address_id, gender)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (provider_id) DO UPDATE
        SET provider_first_name = EXCLUDED.provider_first_name,
            provider_last_name = EXCLUDED.provider_last_name,
            degree = EXCLUDED.degree,
            network = EXCLUDED.network,
            claim_id = EXCLUDED.claim_id,
            practice_name = EXCLUDED.practice_name,
            address_id = EXCLUDED.address_id,
            gender = EXCLUDED.gender;
        """, (provider_id, provider_first_name, provider_last_name, degree, network, claim_id, 
              practice_name, address_id, gender))


# Import member data from csv file
with open('Datasets/mem_vf.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        member_id = int(row['member_id'])
        member_first_name = row['member_first_name']
        member_last_name = row['member_last_name']
        address_id = int(row['address_id'])
        gender = row['gender']
        member_dob = row['member_dob']  
        day_memberD = int(row['day_memberD'])
        month_memberD = int(row['month_memberD'])
        year_memberD = int(row['year_memberD'])
        claim_id = int(row['claim_id'])
        coverage_id = int(row['coverage_id'])

        cur.execute("""
        INSERT INTO insurance.member (member_id, member_first_name, member_last_name, address_id, gender, 
                    member_dob, day_memberD, month_memberD, year_memberD, claim_id, coverage_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (member_id) DO UPDATE
        SET member_first_name = EXCLUDED.member_first_name,
            member_last_name = EXCLUDED.member_last_name,
            address_id = EXCLUDED.address_id,
            gender = EXCLUDED.gender,
            member_dob = EXCLUDED.member_dob,
            day_memberD = EXCLUDED.day_memberD,
            month_memberD = EXCLUDED.month_memberD,
            year_memberD = EXCLUDED.year_memberD,
            claim_id = EXCLUDED.claim_id,
            coverage_id = EXCLUDED.coverage_id;
        """, (member_id, member_first_name, member_last_name, address_id, gender, member_dob, 
              day_memberD, month_memberD, year_memberD, claim_id, coverage_id))


# Import status data from csv file
with open('Datasets/st.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        status_id = int(row['status_id'])
        claim_status = row['claim_status']
        type_status = row['type']

        cur.execute("""
        INSERT INTO insurance.status (status_id, claim_status, type)
        VALUES (%s, %s, %s)
        ON CONFLICT (status_id) DO UPDATE
        SET claim_status = EXCLUDED.claim_status,
            type = EXCLUDED.type;
        """, (status_id, claim_status, type_status))


# Commit the transaction
conn.commit()