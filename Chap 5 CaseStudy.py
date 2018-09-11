# Import create_engine, MetaData
from sqlalchemy import create_engine, MetaData

# Define an engine to connect to chapter5.sqlite: engine
engine = create_engine('sqlite:///chapter5.sqlite')

# Initialize MetaData: metadata
metadata = MetaData()
#..................................
##################################

# Import Table, Column, String, and Integer
from sqlalchemy import Table,Column,String,Integer

# Build a census table: census
census = Table('census', metadata,
               Column('state', String(30)),
               Column('sex', String(1)),
               Column('age',Integer()),
               Column('pop2000',Integer()),
               Column('pop2008',Integer()))

# Create the table in the database
metadata.create_all(engine)
#..................................
###################################

# Create an empty list: values_list
values_list = []

# Iterate over the rows
for row in csv_reader:
    # Create a dictionary with the values
    data = {'state': row[0], 'sex': row[1], 'age':row[2], 'pop2000': row[3],
            'pop2008': row[4]}
    # Append the dictionary to the values list
    values_list.append(data)
#.........................
##########################

# Import insert
from sqlalchemy import insert

# Build insert statement: stmt
stmt=insert(census)

# Use values_list to insert data: results
results=connection.execute(stmt,values_list)

# Print rowcount
print(results.rowcount)
#................................
<script.py> output:
    8772
#################################

# Import select
from sqlalchemy import select

# Calculate weighted average age: stmt
stmt = select([census.columns.sex,
               (func.sum(census.columns.pop2008 * census.columns.age) /
                func.sum(census.columns.pop2008)).label('average_age')
               ])

# Group by sex
stmt = stmt.group_by(census.columns.sex)

# Execute the query and store the results: results
results = connection.execute(stmt).fetchall()

# Print the average age by sex
for result in results:
    print(result.sex, result.average_age)
#.......................................
<script.py> output:
    F 38
    M 35
########################################

# import case, cast and Float from sqlalchemy
from sqlalchemy import case, cast, Float

# Build a query to calculate the percentage of females in 2000: stmt
stmt = select([census.columns.state,
    (func.sum(
        case([
            (census.columns.sex == 'F', census.columns.pop2000)
        ], else_=0)) /
     cast(func.sum(census.columns.pop2000), Float) * 100).label('percent_female')
])

# Group By state
stmt = stmt.group_by(census.columns.state)

# Execute the query and store the results: results
results = connection.execute(stmt).fetchall()

# Print the percentage
for result in results:
    print(result.state, result.percent_female)
#...........................
<script.py> output:
    Alabama 51.8324077702
    Alaska 49.3014978935
    Arizona 50.2236130306
    Arkansas 51.2699284622
    California 50.3523321490
    Colorado 49.8476706030
    Connecticut 51.6681650713
    Delaware 51.6110973356
    District of Columbia 53.1296261417
    Florida 51.3648800117
    Georgia 51.1140835034
    Hawaii 51.1180118369
    Idaho 49.9897262390
    Illinois 51.1122423480
    Indiana 50.9548031330
    Iowa 50.9503983425
    Kansas 50.8218641078
    Kentucky 51.3268703693
    Louisiana 51.7535159655
    Maine 51.5057081342
    Maryland 51.9357554997
    Massachusetts 51.8430235713
    Michigan 50.9724651832
    Minnesota 50.4933294430
################################

# Build query to return state name and population difference from 2008 to 2000
stmt = select([census.columns.state,
     (census.columns.pop2008-census.columns.pop2000).label('pop_change')
])

# Group by State
stmt = stmt.group_by(census.columns.state)

# Order by Population Change
stmt = stmt.order_by(desc('pop_change'))

# Limit to top 10
stmt = stmt.limit(10)

# Use connection to execute the statement and fetch all results
results = connection.execute(stmt).fetchall()

# Print the state and population change for each record
for result in results:
    print('{}:{}'.format(result.state, result.pop_change))
#...................................
<script.py> output:
    California:105705
    Florida:100984
    Texas:51901
    New York:47098
    Pennsylvania:42387
    Arizona:29509
    Ohio:29392
    Illinois:26221
    Michigan:25126
    North Carolina:24108
##############################################################3
