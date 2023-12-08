import sqlite3

# database connection/creation
database = sqlite3.connect('linkedIn.db')

# cursor
cursor = database.cursor()


# Function creates all the tables required to run the program
def createTables():

    createIndustriesTable()

    createCountriesTable()

    createStatesTable()

    createCitiesTable()

    createCompaniesTable()

    createCompanySpecialtyTable()

    createJobPostingsTable()

    createJobIndustryTable()

    createJobBenefitsTable()

    createJobSkillsTable()

    pass


def createCompaniesTable():
    # Function creates the companies table
    cursor.execute("""
                   CREATE TABLE Companies (
                       company_id INTEGER NOT NULL,
                       name TEXT NOT NULL,
                       company_size INTEGER,
                       city_id INTEGER,
                       address TEXT,
                       industry_id INTEGER NOT NULL,
                       employee_count INTEGER NOT NULL,
                       follower_count INTEGER NOT NULL,
                       time_recorded REAL NOT NULL,
                       PRIMARY KEY(company_id),
                       FOREIGN KEY(city_id) REFERENCES Cities(cities_id),
                       FOREIGN KEY(industry_id) REFERENCES Industries(industry_id)
                   );
                   """)
    database.commit()


def createIndustriesTable():
    # Function creates the company industry table
    cursor.execute("""
                   CREATE TABLE Industries (
                       industry_id INTEGER NOT NULL,
                       industry TEXT,
                       PRIMARY KEY(industry_id)
                   );
                   """)
    database.commit()


def createCitiesTable():
    # Function creates the cities table
    cursor.execute("""
                   CREATE TABLE Cities (
                       cities_id INTEGER NOT NULL,
                       city TEXT NOT NULL,
                       state_id INTEGER NOT NULL,    
                       PRIMARY KEY(cities_id),
                       FOREIGN KEY(state_id) REFERENCES States(state_id)
                   );
                   """)
    database.commit()


def createStatesTable():
    # Function creates the states table
    cursor.execute("""
                   CREATE TABLE States (
                       states_id INTEGER NOT NULL,
                       state TEXT NOT NULL,
                       country_id INTEGER NOT NULL,    
                       PRIMARY KEY(states_id),
                       FOREIGN KEY(country_id) REFERENCES Countries(country_id)
                   );
                   """)
    database.commit()


def createCountriesTable():
    # Function creates the Countries table
    cursor.execute("""
                   CREATE TABLE Countries (
                       country_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                       country TEXT NOT NULL UNIQUE
                   );
                   """)
    database.commit()


def createCompanySpecialtyTable():
    # Function creates the company speciality table
    cursor.execute("""
                   CREATE TABLE Company_Speciality (
                       company_id INTEGER NOT NULL,             
                       speciality TEXT NOT NULL,
                       PRIMARY KEY(company_id),
                       FOREIGN KEY(company_id) REFERENCES Companies(company_id)
                   );
                   """)
    database.commit()


def createJobPostingsTable():
    # Function creates the job postings table

    cursor.execute("""
                   CREATE TABLE Job_Postings (
                       job_id INTEGER NOT NULL,
                       currency TEXT NOT NULL,
                       applies INTEGER NOT NULL,
                       work_type TEXT NOT NULL,
                       expiry REAL NOT NULL,
                       pay_period TEXT NOT NULL,
                       location TEXT NOT NULL,
                       company_id INTEGER NOT NULL,          
                       original_listed_time REAL NOT NULL,
                       listed_time REAL NOT NULL,
                       med_salary INTEGER NOT NULL,
                       remote_allowed INTEGER NOT NULL,
                       views INTEGER NOT NULL,
                       formatted_work_type TEXT NOT NULL,
                       application_type TEXT NOT NULL,
                       max_salary INTEGER NOT NULL,
                       min_salary INTEGER NOT NULL,
                       skill_desc TEXT NOT NULL,
                       sponsored INTEGER NOT NULL,
                       closed_time REAL NOT NULL,
                       formatted_experience_level TEXT NOT NULL,
                       compensation_type TEXT NOT NULL,
                       PRIMARY KEY(job_id),
                       FOREIGN KEY(company_id) REFERENCES Companies(company_id)
                   );
                   """)
    database.commit()


def createJobIndustryTable():
    # Function creates the job industries table
    cursor.execute("""
                   CREATE TABLE Job_Industry (
                       job_id INTEGER NOT NULL,
                       industry_id INTEGER NOT NULL,        
                       PRIMARY KEY(job_id),
                       FOREIGN KEY(job_id) REFERENCES Job_Postings(job_id),
                       FOREIGN KEY(industry_id) REFERENCES Industries(industry_id)
                   );
                   """)
    database.commit()


def createJobBenefitsTable():
    # Function creates the job benefits table
    cursor.execute("""
                   CREATE TABLE Job_Benefits (
                       job_id INTEGER NOT NULL,            
                       inferred INTEGER NOT NULL,
                       type TEXT NOT NULL,
                       PRIMARY KEY(job_id),
                       FOREIGN KEY(job_id) REFERENCES Job_Postings(job_id)
                   );
                   """)
    database.commit()


def createJobSkillsTable():
    # Function creates the job skills table
    cursor.execute("""
                   CREATE TABLE Job_Skills (
                       job_id INTEGER NOT NULL,             
                       skill_abr TEXT NOT NULL,
                       PRIMARY KEY(job_id),
                       FOREIGN KEY(job_id) REFERENCES Job_Postings(job_id)
                   );
                   """)
    database.commit()


if __name__ == '__main__':
    createTables()
    # insertTables()
