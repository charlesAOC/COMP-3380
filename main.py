import sqlite3
from src.CreateTables import createTables, dropTables
from src.insertData import insertTables
from src.basicQuery import showAllTables, exposeTable


# database connection/creation
database = sqlite3.connect('linkedIn.db')

# cursor
cursor = database.cursor()


def main():

    # created the tables
    createTables()

    # populates the tables
    insertTables()

    # starts the command line user program loop
    runCommandLine()

    # drop tables at the end of each run
    dropTables()


def runCommandLine():
    # method performs the user interaction loops

    print("\nWelcome To Command Line Interface")
    # print("Enter 'exit' to quit")
    # print("For a list of commands enter 'h'")

    while True:
        print("\n\nEnter 'exit' to quit")
        print("For a list of commands enter 'h'")

        cursor = database.cursor()
        command = input("Enter a Command: ")

        if command.lower() == 'h':
            print("'1' -- Top two work types according to job postings for each industry")
            print("'2' -- Most sought after skills in industries")
            print("'3' -- Industries with the highest number of applications")
            print("'4' -- Cities with most job postings")
            print(
                "'5' -- Top 3 Industries with the highest average job views per posting")
            print("'6' -- Top 3 cities with the highest number of unique industries")
            print("'7' -- View all Tables in the Database")

            print("'q' -- View Industries table")
            print("'w' -- View Countries table")
            print("'e' -- View States table")
            print("'r' -- View Cities table")
            print("'t' -- View Company_Count table")
            print("'y' -- View Company_Industries table")
            print("'u' -- View Companies table")
            print("'i' -- View Company_Speciality table")
            print("'o' -- View Salaries table")
            print("'p' -- View Job_Postings table")
            print("'a' -- View Job_Industry table")
            print("'s' -- View Job_Benefits table")
            print("'d' -- View Job_Skills table")

            print("'exit' -- Exit Command Line Interface")

        if command == '1':
            try:

                print("\n Searching...\n")
                query = """
                    WITH RankedWorkTypes AS (
                    SELECT
                        Job_Industry.industry_id,
                        Job_Postings.work_type,
                        ROW_NUMBER() OVER(PARTITION BY Job_Industry.industry_id ORDER BY COUNT(Job_Postings.job_id) DESC) AS type_rank
                    FROM Job_Industry
                    JOIN Job_Postings ON Job_Industry.job_id = Job_Postings.job_id
                    GROUP BY Job_Industry.industry_id, Job_Postings.work_type
                    )
                    SELECT
                        Industries.industry,
                        RankedWorkTypes.work_type,
                        COUNT(Job_Postings.job_id) AS num_job_postings
                    FROM RankedWorkTypes
                    JOIN Industries ON RankedWorkTypes.industry_id = Industries.industry_id
                    JOIN Job_Industry ON RankedWorkTypes.industry_id = Job_Industry.industry_id
                    JOIN Job_Postings ON Job_Industry.job_id = Job_Postings.job_id
                    WHERE RankedWorkTypes.type_rank <= 2
                    GROUP BY Industries.industry, RankedWorkTypes.work_type
                    LIMIT 2
                    """
                cursor.execute(query)
                myresult = cursor.fetchall()

                for x in myresult:
                    print(x)
            except ValueError:
                print("Query Failed")

        if command == '2':
            try:
                print("\n Searching...\n")
                query = """
                    WITH SkillDemand AS (
                        SELECT
                            Job_Industry.industry_id,
                            Job_Skills.skill_abr,
                            COUNT(Job_Postings.job_id) AS skill_count,
                            ROW_NUMBER() OVER(PARTITION BY Job_Skills.skill_abr ORDER BY COUNT(Job_Postings.job_id) DESC) AS industry_rank
                        FROM Job_Industry
                        JOIN Job_Skills ON Job_Industry.job_id = Job_Skills.job_id
                        JOIN Job_Postings ON Job_Industry.job_id = Job_Postings.job_id
                        GROUP BY Job_Industry.industry_id, Job_Skills.skill_abr
                    )
                    SELECT
                        Industries.industry,
                        SkillDemand.skill_abr,
                        SkillDemand.skill_count
                    FROM SkillDemand
                    JOIN Industries ON SkillDemand.industry_id = Industries.industry_id
                    WHERE SkillDemand.industry_rank <= 2
                    """
                cursor.execute(query)
                myresult = cursor.fetchall()

                for x in myresult:
                    print(x)

            except ValueError:
                print("Query Failed")

        if command == '3':
            try:
                print("\n Searching...\n")
                query = """
                    SELECT
                        Industries.industry,
                        AVG(Job_Postings.applies) AS avg_applications
                    FROM Industries
                    JOIN Job_Industry ON Industries.industry_id = Job_Industry.industry_id
                    JOIN Job_Postings ON Job_Industry.job_id = Job_Postings.job_id
                    GROUP BY Industries.industry
                    ORDER BY avg_applications DESC
                    LIMIT 10
                    """
                cursor.execute(query)
                myresult = cursor.fetchall()

                for x in myresult:
                    print(x)

            except ValueError:
                print("Query Failed")

        if command == '4':
            try:
                print("\n Searching...\n")
                query = """
                    SELECT 
                        Cities.city, 
                        COUNT(Job_Postings.job_id) AS num_job_postings
                    FROM Cities
                    JOIN Companies ON Cities.cities_id = Companies.city_id
                    JOIN Job_Postings ON Companies.company_id = Job_Postings.company_id
                    GROUP BY Cities.city
                    ORDER BY num_job_postings DESC
                    LIMIT 3
                    """

                cursor.execute(query)
                myresult = cursor.fetchall()

                for x in myresult:
                    print(x)

            except ValueError:
                print("Query Failed")

        if command == '5':
            try:
                print("\n Searching...\n")
                query = """
                    SELECT 
                        Industries.industry,
                        AVG(Job_Postings.views) AS avg_job_views
                    FROM Industries
                    JOIN Job_Industry ON Industries.industry_id = Job_Industry.industry_id
                    JOIN Job_Postings ON Job_Industry.job_id = Job_Postings.job_id
                    GROUP BY Industries.industry
                    ORDER BY avg_job_views DESC
                    LIMIT 3
                    """
                cursor.execute(query)
                myresult = cursor.fetchall()

                for x in myresult:
                    print(x)
            except ValueError:
                print("Query Failed")

        if command == '6':
            try:
                print("\n Searching...\n")
                query = """
                    SELECT 
                        Cities.city,
                        COUNT(DISTINCT Industries.industry_id) AS num_unique_industries
                    FROM Cities
                    JOIN Companies ON Cities.cities_id = Companies.city_id
                    JOIN company_industries ON Companies.company_id = company_industries.company_id
                    JOIN Industries ON company_industries.industry_id = industries.industry_id
                    GROUP BY Cities.city
                    ORDER BY num_unique_industries DESC
                    LIMIT 3
                    """

                cursor.execute(query)
                myresult = cursor.fetchall()

                for x in myresult:
                    print(x)

            except ValueError:
                print("Query Failed")

        if command == '7':
            print("\n loading...")
            print("\n---Tables---")
            showAllTables()

        if command == 'q':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Industries', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'w':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Countries', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'e':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('States', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'r':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Cities', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 't':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Company_Count', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'y':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Company_Industries', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'u':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Companies', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'i':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Company_Speciality', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'o':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Salaries', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'p':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Job_Postings', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'a':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Job_Industry', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 's':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Job_Benefits', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command == 'd':
            try:
                print("\n Searching...\n")
                limit_value = int(
                    input("Enter the amount of rows you would like: "))

                exposeTable('Job_Skills', limit_value)

            except ValueError:
                print("Please Enter a valid integer")

        if command.lower() == 'exit':
            break


if __name__ == '__main__':
    main()
