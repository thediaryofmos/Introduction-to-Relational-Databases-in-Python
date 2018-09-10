# Import create_engine function
from sqlalchemy import create_engine

# Create an engine to the census database
engine = create_engine('mysql+pymysql://'+'student:datacamp'+'@courses.csrrinzqubik.us-east-1.rds.amazonaws.com:3306/'+'census')

# Print the table names
print(engine.table_names())
#................................
<script.py> output:
    ['census', 'state_fact']
####################################

# Build query to return state names by population difference from 2008 to 2000: stmt
stmt = select([census.columns.state, (census.columns.pop2008-census.columns.pop2000).label('pop_change')])

# Append group by for the state: stmt
stmt = stmt.group_by(census.columns.state)

# Append order by for pop_change descendingly: stmt
stmt = stmt.order_by(desc('pop_change'))

# Return only 5 results: stmt
stmt = stmt.limit(5)

# Use connection to execute the statement and fetch all results
results = connection.execute(stmt).fetchall()

# Print the state and population change for each record
for result in results:
    print('{}:{}'.format(result.state, result.pop_change))
#..................................................
<script.py> output:
    California:105705
    Florida:100984
    Texas:51901
    New York:47098
    Pennsylvania:42387
###########################################################

# import case, cast and Float from sqlalchemy
from sqlalchemy import case, cast, Float

# Build an expression to calculate female population in 2000
female_pop2000 = func.sum(
    case([
        (census.columns.sex == 'F', census.columns.pop2000)
    ], else_=0))

# Cast an expression to calculate total population in 2000 to Float
total_pop2000 = cast(func.sum(census.columns.pop2000), Float)

# Build a query to calculate the percentage of females in 2000: stmt
stmt = select([female_pop2000 / total_pop2000* 100])

# Execute the query and store the scalar result: percent_female
percent_female = connection.execute(stmt).scalar()

# Print the percentage
print(percent_female)
#................................................
<script.py> output:
    51.0946743229
###########################################

# Build a statement to join census and state_fact tables: stmt
stmt = select([census.columns.pop2000, state_fact.columns.abbreviation])

# Execute the statement and get the first result: result
result = connection.execute(stmt).first()

# Loop over the keys in the result object and print the key and value
for key in result.keys():
    print(key, getattr(result, key))
#............................................
pop2000 89600
abbreviation IL
############################################

# Build a statement to select the census and state_fact tables: stmt
stmt = select([census, state_fact])

# Add a select_from clause that wraps a join for the census and state_fact
# tables where the census state column and state_fact name column match
stmt = stmt.select_from(
    census.join(state_fact, census.columns.state == state_fact.columns.name))

# Execute the statement and get the first result: result
result = connection.execute(stmt).first()

# Loop over the keys in the result object and print the key and value
for key in result.keys():
    print(key, getattr(result, key))
#...................................................
<script.py> output:
    state Illinois
    sex M
    age 0
    pop2000 89600
    pop2008 95012
    id 13
    name Illinois
    abbreviation IL
    country USA
    type state
    sort 10
    status current
    occupied occupied
    notes 
    fips_state 17
    assoc_press Ill.
    standard_federal_region V
    census_region 2
    census_region_name Midwest
    census_division 3
    census_division_name East North Central
    circuit_court 7
###################################

# Build a statement to select the state, sum of 2008 population and census
# division name: stmt
stmt = select([
    census.columns.state,
    func.sum(census.columns.pop2008),
    state_fact.columns.census_division_name
])

# Append select_from to join the census and state_fact tables by the census state and state_fact name columns
stmt = stmt.select_from(
    census.join(state_fact, census.columns.state == state_fact.columns.name)
)

# Append a group by for the state_fact name column
stmt = stmt.group_by(state_fact.columns.name)

# Execute the statement and get the results: results
results = connection.execute(stmt).fetchall()

# Loop over the the results object and print each record.
for record in results:
    print(record)
#............................................
<script.py> output:
    ('Alabama', 4649367, 'East South Central')
    ('Alaska', 664546, 'Pacific')
    ('Arizona', 6480767, 'Mountain')
    ('Arkansas', 2848432, 'West South Central')
####################################################

# Make an alias of the employees table: managers
managers = employees.alias()

# Build a query to select manager's and their employees names: stmt
stmt = select(
    [managers.columns.name.label('manager'),
     employees.columns.name.label('employee')]
)

# Match managers id with employees mgr: stmt
stmt = stmt.where(managers.columns.id == employees.columns.mgr)

# Order the statement by the managers name: stmt
stmt = stmt.order_by(managers.columns.name)

# Execute statement: results
results = connection.execute(stmt).fetchall()

# Print records
for record in results:
    print(record)
#................................................
<script.py> output:
    ('FILLMORE', 'GRANT')
    ('FILLMORE', 'ADAMS')
    ('FILLMORE', 'MONROE')
    ('GARFIELD', 'JOHNSON')
    ('GARFIELD', 'LINCOLN')
    ('GARFIELD', 'POLK')
    ('GARFIELD', 'WASHINGTON')
    ('HARDING', 'TAFT')
    ('HARDING', 'HOOVER')
    ('JACKSON', 'HARDING')
    ('JACKSON', 'GARFIELD')
    ('JACKSON', 'FILLMORE')
    ('JACKSON', 'ROOSEVELT')
##################################################

# Make an alias of the employees table: managers
managers = employees.alias()

# Build a query to select managers and counts of their employees: stmt
stmt = select([managers.columns.name, func.count(employees.columns.id)])

# Append a where clause that ensures the manager id and employee mgr are equal
stmt = stmt.where(managers.columns.id==employees.columns.mgr)

# Group by Managers Name
stmt = stmt.group_by(managers.columns.name)

# Execute statement: results
results = connection.execute(stmt).fetchall()

# print manager
for record in results:
    print(record)
#....................................................
<script.py> output:
    ('FILLMORE', 3)
    ('GARFIELD', 4)
    ('HARDING', 2)
    ('JACKSON', 4)
####################################################

# Start a while loop checking for more results
while more_results:
    # Fetch the first 50 results from the ResultProxy: partial_results
    partial_results = results_proxy.fetchmany(50)

    # if empty list, set more_results to False
    if partial_results == []:
        more_results = False

    # Loop over the fetched records and increment the count for the state
    for row in partial_results:
        if row.state in state_count:
            state_count[row.state] +=1
        else:
            state_count[row.state]=1

# Close the ResultProxy, and thus the connection
results_proxy.close()

# Print the count by state
print(state_count)
#...........................................................
<script.py> output:
    {'Maryland': 49, 'Illinois': 172, 'North Dakota': 75, 'Idaho': 172, 'Florida': 172, 'Massachusetts': 16, 'District of Columbia': 172, 'New Jersey': 172}
##############################################################
