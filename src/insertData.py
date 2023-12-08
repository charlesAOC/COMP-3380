import sqlite3

# database connection/creation
database = sqlite3.connect('linkedIn.db')

# cursor
cursor = database.cursor()


def insertTables():
    # insertIndustriesTable()
    # insertCountriesTable()
    # insertStatesTable()
    # insertCitiesTable()
    # insertCompanyIndustriesTable()
    # insertCompanyCountsTable()

    pass


def readFile(filename: str):
    # reads files
    lines = ''

    with open(filename, encoding="utf8") as f:
        lines = f.readlines()
        lines = [l.strip().split(',') for l in lines]
        # [print(l) for l in lines]

    return lines[1:]


def insertCompaniesTable():
    # Function creates the companies table
    pass


def getIndustriesId(industry: str):
    result = ''
    # fmt =

    if industry != '':
        command = 'SELECT industry_id FROM Industries WHERE industry = "{}";'.format(
            industry)
        cursor.execute(command)

        result = cursor.fetchone()

        if (result != None and len(result) > 0):
            result = result[0]
        elif result == None:
            result = ''

        # print('result - ', result)

    return result


def insertCompanyIndustriesTable():
    # Function creates the companies_Industry table
    lines = readFile('archive/company_details/company_industries.csv')

    fmt = 'INSERT INTO Company_Industries VALUES ({}, {});'
    fmt_null = 'INSERT INTO Company_Industries VALUES ({}, NULL);'

    for l in lines:
        industryId = getIndustriesId(l[1])

        if industryId != '':
            cursor.execute(fmt.format(l[0], industryId))
        else:
            cursor.execute(fmt_null.format(l[0]))
    database.commit()


def insertCompanyCountsTable():
    # Function creates the companies_count table
    lines = readFile('archive/company_details/employee_counts.csv')

    fmt = 'INSERT INTO Company_Count VALUES ({}, {}, {}, {});'

    [cursor.execute(fmt.format(l[0], l[1], l[2], l[3])) for l in lines]
    database.commit()


def insertIndustriesTable():
    # Function creates the company industry table
    lines = readFile('archive/maps/industries.csv')

    fmt = 'INSERT INTO Industries VALUES ({}, "{}");'
    fmt_null = 'INSERT INTO Industries VALUES ({}, NULL);'

    [cursor.execute(fmt.format(l[0], l[1])) if l[1] != '' else cursor.execute(fmt_null.format(l[0]))
     for l in lines]

    database.commit()


def insertCitiesTable():
    # Function creates the cities table
    lines = readFile('archive/company_details/city_state.csv')

    fmt = 'INSERT INTO Cities(city, state_id) VALUES ("{}", {});'
    fmt_null = 'INSERT INTO Cities(city, state_id) VALUES ("{}", NULL);'

    # format for state-country pair
    setfmt = "{}-{}"

    # set to find unique states
    tempSet = set()

    for l in lines:
        city = l[0]

        if city != '':
            # get country_id
            country = getCountryID(l[2]) if l[2] != '' else ''

            # get state_id
            state = getStateId(l[1], country) if l[1] else ''

            entry = setfmt.format(city, state)

            # add it to set then add to table if the pairing hasnt been prev encountered
            if entry not in tempSet:
                tempSet.add(entry)

                if state != '':
                    cursor.execute(fmt.format(city, state))
                    # print(fmt.format(city, state))
                else:
                    cursor.execute(fmt_null.format(city))
                    # print(fmt_null.format(city))

    database.commit()


def getStateId(state: str, countryId: str):
    # gets the state_id

    if countryId != '':
        command = 'SELECT states_id FROM States WHERE state = "{}" AND country_id = {};'.format(
            state, countryId)
    else:
        command = 'SELECT states_id FROM States WHERE state = "{}" AND country_id = NULL;'.format(
            state, countryId)

    cursor.execute(command)

    tempResult = cursor.fetchone()

    result = ''

    if (tempResult != None and len(tempResult) > 0):
        # print(temp[0])
        result = tempResult[0]

    return result


def insertStatesTable():
    # Function creates the states table
    lines = readFile('archive/company_details/states_country.csv')

    fmt = 'INSERT INTO States(state, country_id) VALUES ("{}", "{}");'
    fmt_null = 'INSERT INTO States(state, country_id) VALUES ("{}", NULL);'

    # format for state-country pair
    setfmt = "{}-{}"

    # set to find unique states
    tempSet = set()

    for l in lines:
        state = l[0]
        country = l[1] if l[1] != '' else ''

        # if the states col is not blank
        if (state != ''):
            entry = setfmt.format(state, country)

            # add it to set then add to table if the pairing hasnt been prev encountered
            if (entry not in tempSet):
                tempSet.add(entry)
                # print(fmt.format(l[0], l[1] if l[1] != '' else 'NULL'))

                # if there is a country get the country_id otherwise set country_id to null
                if country != '':
                    cursor.execute(fmt.format(l[0], getCountryID(l[1])))
                else:
                    cursor.execute(fmt_null.format(l[0]))

    database.commit()


def getCountryID(country: str):
    # gets the country_id, ALl the countries in the dataset are mapped so this should always return something if used correctly

    command = 'SELECT country_id FROM Countries WHERE country = "{}";'.format(
        country)

    cursor.execute(command)

    temp = cursor.fetchone()

    result = ''

    if (temp != None and len(temp) > 0):
        # print(temp[0])
        result = temp[0]

    return result


def insertCountriesTable():
    # Function creates the Countries table
    lines = readFile('archive/company_details/countries.csv')

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
