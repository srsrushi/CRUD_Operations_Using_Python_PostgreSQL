import sqlalchemy
from urllib.parse import quote
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey,inspect,text,select

#print (sqlalchemy.__version__)

db_string = 'postgresql://postgres:%s@localhost:5432/Tutorials'% quote('SRS@postgres7')
engine = create_engine(db_string)
meta = MetaData(engine)  
MetaData.reflect(meta)
inspector = inspect(engine)

def create_table(table_name,columns):
	"""
	table_name : defines table name of the table to be created
	columns : list of the columns of the table to be created
	"""	
	try:
		if inspector.has_table(table_name)==True:
			raise Exception

	except Exception as e:
		print(table_name+" table already exists")

	else:
		table = Table(table_name,meta,*columns)
		with engine.connect() as conn:
			table.create()

def insert_records(table_name,records):
	"""
	table_name : defines table name of the table in which the records are to be inserted
	records : list of the records to be inserted e.g. values=[{'first_name':"Clan", 'last_name':"Calvin"},{'first_name':"Dev", 'last_name':"Sen"}]
	"""	
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]
		insert_statement = table.insert().values(records)
		with engine.connect() as conn:
			conn.execute(insert_statement)

def update_records(table_name,column_name,column_value,condition_operator,records):
	"""
	table_name : defines table name of the table in which the records are to be updated
	column_name : name of the column to be filtered while updation e.g. customer_id
	column_value : value of the column name to be filtered while updation
	condition_operator : filter operator for the updation e.g. =, !=, <,>
	records : list of the records to be inserted e.g. values=[{'first_name':"Clan", 'last_name':"Calvin"},{'first_name':"Dev", 'last_name':"Sen"}]
	"""	
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]

		condition = text("{} {} {}".format(column_name,condition_operator,column_value))

		update_statement = table.update().where(condition).values(records)

		with engine.connect() as conn:
			conn.execute(update_statement)

def delete_records(table_name,column_name,column_value,condition_operator):
	"""
	table_name : defines table name of the table to be deleted
	column_name : name of the column to be filtered while deletion e.g. customer_id
	column_value : value of the column name to be filtered while deletion
	condition_operator : filter operator for the deletion e.g. =, !=, <,>
	"""	
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]

		condition = text("{} {} {}".format(column_name,condition_operator,column_value))

		delete_statement = table.delete().where(condition)

		with engine.connect() as conn:
			conn.execute(delete_statement)

def delete_all_records(table_name):
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]

		truncate_query = text('TRUNCATE TABLE public."{}";'.format(table_name))

		with engine.connect() as conn:
			conn.execute(truncate_query)

def drop_table(table_name):
	table = ""
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]

		with engine.connect() as conn:
			table.drop(conn)

def fetch_all_records(table_name):
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]

		fetch_statement = table.select()

		with engine.connect() as conn:
			result_set = conn.execute(fetch_statement)
			return result_set

def conditional_fetch(table_name,column_name,column_value,condition_operator):
	"""
	table_name : name of the table from which records are to be fetched
	column_name : name of the column to be filtered while fetching e.g. customer_id
	column_value : value of the column name to be filtered while fetching
	condition_operator : filter operator for the fetching e.g. =, !=, <,>
	"""	
	try:
		if inspector.has_table(table_name)==False:
			raise sqlalchemy.exc.NoSuchTableError

	except sqlalchemy.exc.NoSuchTableError as e:
		print(table_name+" table not found")

	else:
		table = meta.tables[table_name]

		condition = text("{} {} {}".format(column_name,condition_operator,column_value))

		fetch_statement = select([table]).where(condition)

		with engine.connect() as conn:
			result_set = conn.execute(fetch_statement)
			return result_set.fetchall()
			

#Calling the create_table function
columns = [Column('customer_id',Integer,primary_key=True,autoincrement=True),
				Column('first_name',String),
				Column('last_name',String),
  				Column('username',String), 
  				Column('email',String),
  				Column('address',String),
  				Column('town',String)]
create_table('Customer',columns)

#calling insert_records function
values=[{'first_name':"Clan", 'last_name':"Calvin", 'username' : 'clancalvin','email':'clancalvin@company.com','address':'Av Town, U.S. Post 21','town':'AV'},{'first_name':"Flan", 'last_name':"Falvin", 'username' : 'flancalvin','email':'flancalvin@company.com','address':'Av Town, U.S. Post 21','town':'AV'}]
insert_records("Customer",values)

#calling update_records function
values={'first_name':"zlan", 'last_name':"zalker"}
update_records("Customer","customer_id",3,">",values)

#calling fetch_all_records function:
result_set = fetch_all_records('Customer')
for result in result_set:
	print(result)

#calling fetch_all_records function:
result_set = conditional_fetch('Customer',"customer_id",1,">")
for result in result_set:
	print(result)

#calling delete_records function
delete_records("Customer","customer_id",3,">")

#calling delete_all_records function
delete_all_records("Customer")

#calling drop_table function
drop_table('Customer')

