import sqlite3

# database connection/creation
database = sqlite3.connect('linkedIn.db')

# cursor
cursor = database.cursor()


def insertTables():
    # create all tables

    insertIndustriesTable()
    insertCountriesTable()
    insertStatesTable()
    insertCitiesTable()
    insertCompanyIndustriesTable()
    insertCompanyCountsTable()
    insertCompaniesTable()
    insertCompanySpecialtyTable()
    insertSalariesTable()
    insertJobPostingsTable()
    insertJobIndustryTable()
    insertJobBenefitsTable()
    insertJobSkillsTable()


def readFile(filename: str):
    # reads files
    lines = ''

    with open(filename, encoding="utf8") as f:
        lines = f.readlines()
        lines = [l.strip().split(',') for l in lines]

    return lines[1:]


def insertCompaniesTable():
    # Function creates the companies table
    lines = readFile('archive/company_details/companies.csv')

    fmt = 'INSERT INTO Companies VALUES ({}, "{}", {}, {}, "{}");'
    fmt_null = 'INSERT INTO Companies VALUES ({}, "{}", {}, {}, NULL);'

    for l in lines:
        company_id = l[0]
        name = l[1]
        companySize = l[2] if l[2] != '' else 'NULL'
        state = l[3]
        country = l[4]
        city = l[5]
        address = l[6].strip()

        if city != '':
            # get country_id
            country = getCountryID(country) if country != '' else ''

            # get state_id
            state = getStateId(state, country) if state else ''

            # get state_id
            city = getCityId(city, state)

            # reset to null if city id not found
            city = city if city != '' else 'NULL'

        else:
            city = 'NULL'

        if address != '':
            # print(fmt.format(company_id, name,
            #                  companySize, city, address))
            cursor.execute(fmt.format(company_id, name,
                                      companySize, city, address))
        else:
            # print(fmt_null.format(
            #     company_id, name, companySize, city))
            cursor.execute(fmt_null.format(
                company_id, name, companySize, city))

    database.commit()


def getIndustriesId(industry: str):
    result = ''

    if industry != '':
        command = 'SELECT industry_id FROM Industries WHERE industry = "{}";'.format(
            industry)
        cursor.execute(command)

        tempRes = cursor.fetchone()

        if (tempRes != None and len(tempRes) > 0):
            result = tempRes[0]

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


def getCityId(city: str, stateId: str):
    result = ''

    if stateId != '':
        command = 'SELECT cities_id FROM Cities WHERE city = "{}" AND state_id = {};'.format(
            city, stateId)
    else:
        command = 'SELECT cities_id FROM Cities WHERE city = "{}" AND state_id = NULL;'.format(
            city, stateId)

    cursor.execute(command)

    tempResult = cursor.fetchone()

    if (tempResult != None and len(tempResult) > 0):
        # print(temp[0])
        result = tempResult[0]

    return result


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
    lines = readFile('archive/company_details/company_specialities.csv')

    fmt = 'INSERT INTO Company_Speciality VALUES ({}, "{}");'

    # [print(fmt.format(l[0], l[1])) for l in lines]
    [cursor.execute(fmt.format(l[0], l[1])) for l in lines]
    database.commit()


def insertSalariesTable():
    lines = readFile('archive/job_details/salaries.csv')

    fmt = 'INSERT INTO Salaries VALUES ({}, {}, {}, {}, {}, "{}", "{}", "{}")'

    for l in lines:
        # print(l)
        salaryId = l[0]
        jobId = l[1]
        maxSalary = l[2] if l[2] != '' else 'NULL'
        medSalary = l[3] if l[3] != '' else 'NULL'
        minSalary = l[4] if l[4] != '' else 'NULL'
        payPeriod = l[5]
        currency = l[6]
        compType = l[7]

        # print(fmt.format(salaryId, jobId, maxSalary, medSalary,
        #       minSalary, payPeriod, currency, compType))
        cursor.execute(fmt.format(salaryId, jobId, maxSalary, medSalary,
                                  minSalary, payPeriod, currency, compType))
    database.commit()


def insertJobPostingsTable():
    # Function creates the job postings table
    lines = readFile('archive/job_postings.csv')
    # [print(l) for l in lines]

    fmt = 'INSERT INTO Job_Postings VALUES ({}, {}, "{}", {}, "{}", {}, {}, {}, {}, {}, "{}", "{}", {}, {}, "{}");'
    fmt_exp = 'INSERT INTO Job_Postings VALUES ({}, {}, "{}", {}, "{}", {}, {}, {}, {}, {}, "{}", "{}", {}, {}, NULL);'

    for l in lines:
        jobId = l[0]
        applies = l[1] if l[1] != '' else 'NULL'
        workTypes = l[2]
        expiry = l[3]
        location = l[4]
        company = l[5] if l[5] != '' else 'NULL'
        origListedTime = l[6]
        listedTime = l[7]
        remote = l[8] if l[8] != '' else 'NULL'
        views = l[9] if l[9] != '' else 'NULL'
        fmtWorkType = l[10]
        appType = l[11]
        sponsored = l[12]
        closedTime = l[13] if l[13] != '' else 'NULL'
        formattedExpLvl = l[14]

        if formattedExpLvl != '':
            cursor.execute(fmt_exp.format(jobId, applies, workTypes,
                                          expiry, location, company, origListedTime, listedTime, remote, views, fmtWorkType, appType,  sponsored, closedTime))
        else:
            cursor.execute(fmt.format(jobId, applies, workTypes,
                                      expiry, location, company, origListedTime, listedTime, remote, views, fmtWorkType, appType,  sponsored, closedTime, formattedExpLvl))

    database.commit()


def insertJobIndustryTable():
    # Function creates the job industries table

    lines = readFile('archive/job_details/job_industries.csv')
    # [print(l) for l in lines]

    fmt = 'INSERT INTO Job_Industry VALUES ({}, {})'

    [cursor.execute(fmt.format(l[0], l[1])) for l in lines]
    database.commit()


def insertJobBenefitsTable():
    # Function creates the job benefits table
    lines = readFile('archive/job_details/benefits.csv')
    # [print(l) for l in lines]

    fmt = 'INSERT INTO Job_Benefits VALUES ({}, {}, "{}")'

    [cursor.execute(fmt.format(l[0], l[1], l[2])) for l in lines]
    database.commit()


def insertJobSkillsTable():
    # Function creates the job skills table
    lines = readFile('archive/job_details/job_skills.csv')
    # [print(l) for l in lines]

    fmt = 'INSERT INTO Job_Skills VALUES ({}, "{}")'

    [cursor.execute(fmt.format(l[0], l[1])) for l in lines]
    database.commit()


if __name__ == '__main__':
    insertTables()
