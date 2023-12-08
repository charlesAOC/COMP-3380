import sqlite3

# database connection/creation
database = sqlite3.connect('linkedIn.db')

# cursor
cursor = database.cursor()


def insertTables():
    # insertIndustriesTable()
    insertCountriesTable()
    pass


def readFile(filename: str):
    # reads files
    lines = ''

    with open(filename) as f:
        lines = f.readlines()
        lines = [l.strip().split(',') for l in lines]
        # [print(l) for l in lines]

    return lines


# 'archive/maps/industries.csv'
def insertCompaniesTable():
    # Function creates the companies table
    pass


def insertIndustriesTable():
    # Function creates the company industry table
    lines = readFile('archive/maps/industries.csv')
    lines = lines[1:]

    fmt = 'INSERT INTO Industries VALUES ({}, "{}");'

    [cursor.execute(fmt.format(l[0], 'NULL' if l[1] == '' else l[1]))
     for l in lines]
    database.commit()


def insertCitiesTable():
    # Function creates the cities table
    pass


def insertStatesTable():
    # Function creates the states table
    pass


def insertCountriesTable():
    # Function creates the Countries table
    lines = readFile('archive/company_details/countries.csv')
    lines = lines[1:]

    fmt = 'INSERT INTO Countries(country) VALUES ("{}");'

    [cursor.execute(fmt.format(l[0])) if l[0] != '' else '' for l in lines]
    database.commit()


def insertCompanySpecialtyTable():
    # Function creates the company speciality table
    pass


def insertJobPostingsTable():
    # Function creates the job postings table
    pass


def insertJobIndustryTable():
    # Function creates the job industries table
    pass


def insertJobBenefitsTable():
    # Function creates the job benefits table
    pass


def insertJobSkillsTable():
    # Function creates the job skills table
    pass


if __name__ == '__main__':
    insertTables()