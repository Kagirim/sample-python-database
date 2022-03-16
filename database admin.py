import mysql.connector
from mysql.connector import Error


class Connection:
    def __init__(self, host, user, database, password):
        self.host = host
        self.user = user
        self.database = database
        self.password = password
        self.my_cursor = None

    def server_connection(self):
        try:
            self.mydb = mysql.connector.connect(host=self.host,
                                                user=self.user,
                                                database=self.database,
                                                password=self.password)

        except Error as err:
            print('Error' + str(err))

    def db_operation(self):
        self.my_cursor = self.mydb.cursor()
        self.my_cursor.execute('SELECT * FROM customers')
        self.my_cursor.execute("CREATE TABLE categories "
                               "(category_id int NOT NULL, "
                               "category_name char(50) NOT NULL, "
                               "CONSTRAINT categories_pk PRIMARY KEY(category_id))")

        self.my_cursor.execute("CREATE TABLE customers "
                               "(customer_id int NOT NULL, "
                               "last_name char(50) NOT NULL, "
                               "first_name char(50) NOT NULL, "
                               "favorite_website char(50), "
                               "CONSTRAINT customers_pk PRIMARY KEY(customer_id));")

        self.my_cursor.execute("CREATE TABLE departments "
                               "(dept_id int NOT NULL, "
                               "dept_name char(50) NOT NULL, "
                               "CONSTRAINT departments_pk PRIMARY KEY(dept_id));")

        self.my_cursor.execute("CREATE TABLE employees "
                               "(employee_number int NOT NULL, "
                               "last_name char(50) NOT NULL, "
                               "first_name char(50) NOT NULL, "
                               "salary int, "
                               "dept_id int, "
                               "CONSTRAINT employees_pk PRIMARY KEY(employee_number));")

        self.my_cursor.execute("CREATE TABLE orders "
                               "(order_id int NOT NULL, "
                               "customer_id int, "
                               "order_date date, "
                               "CONSTRAINT orders_pk PRIMARY KEY (order_id));")

        self.my_cursor.execute("CREATE TABLE products "
                               "(product_id int NOT NULL, "
                               "product_name char(50) NOT NULL, "
                               "category_id int, "
                               "CONSTRAINT products_pk PRIMARY KEY(product_id));")

        self.my_cursor.execute("CREATE TABLE suppliers "
                               "(supplier_id int NOT NULL, "
                               "supplier_name char(50) NOT NULL, "
                               "city char(50), "
                               "state char(50), "
                               "CONSTRAINT suppliers_pk PRIMARY KEY(supplier_id));")

        self.my_cursor.execute("CREATE TABLE summary_data "
                               "(product_id int NOT NULL, "
                               "current_category int, "
                               "CONSTRAINT summary_data_pk PRIMARY KEY (product_id));")

        self.my_cursor.execute("INSERT INTO categories (category_id, category_name) VALUES (25, 'Deli');")

        self.my_cursor.execute("INSERT INTO categories (category_id, category_name) VALUES (50, 'Produce')")

        self.my_cursor.execute("INSERT INTO categories (category_id, category_name) VALUES (75, 'Bakery')")

        self.my_cursor.execute(
            "INSERT INTO categories (category_id, category_name) VALUES (100, 'General Merchandise')")

        self.my_cursor.execute("INSERT INTO categories (category_id, category_name) VALUES (125, 'Technology')")

        self.my_cursor.execute("INSERT INTO customers(customer_id, last_name, first_name, favorite_website) "
                               "VALUES (4000, 'Jackson', 'Joe', 'techonthenet.com')")

        self.my_cursor.execute("INSERT INTO customers (customer_id, last_name, first_name, favorite_website) "
                               "VALUES (5000, 'Smith', 'Jane', 'digminecraft.com'); ")

        self.my_cursor.execute("INSERT INTO customers (customer_id, last_name, first_name, favorite_website) "
                               "VALUES (6000, 'Ferguson', 'Samantha', 'bigactivities.com'); ")

        self.my_cursor.execute("INSERT INTO customers (customer_id, last_name, first_name, favorite_website) "
                               "VALUES (7000, 'Reynolds', 'Allen', 'checkyourmath.com'); ")

        self.my_cursor.execute("INSERT INTO customers (customer_id, last_name, first_name, favorite_website) "
                               "VALUES (8000, 'Anderson', 'Paige', NULL); ")

        self.my_cursor.execute("INSERT INTO customers (customer_id, last_name, first_name, favorite_website) "
                               "VALUES (9000, 'Johnson', 'Derek', 'techonthenet.com'); ")

        self.my_cursor.execute("INSERT INTO departments (dept_id, dept_name) VALUES (500, 'Accounting'); ")

        self.my_cursor.execute("INSERT INTO departments (dept_id, dept_name) VALUES (501, 'Sales'); ")

        self.my_cursor.execute("INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) "
                               "VALUES (1001, 'Smith', 'John', 62000, 500); ")

        self.my_cursor.execute("INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) "
                               "VALUES (1002, 'Anderson', 'Jane', 57500, 500); ")

        self.my_cursor.execute("INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) "
                               "VALUES (1003, 'Everest', 'Brad', 71000, 501); ")

        self.my_cursor.execute("INSERT INTO employees (employee_number, last_name, first_name, salary, dept_id) "
                               "VALUES (1004, 'Horvath', 'Jack', 42000, 501); ")

        self.my_cursor.execute("INSERT INTO orders (order_id, customer_id, order_date) "
                               "VALUES (1,7000,'2016/04/18'); ")

        self.my_cursor.execute("INSERT INTO orders (order_id, customer_id, order_date) "
                               "VALUES (2,5000,'2016/04/18'); ")

        self.my_cursor.execute("INSERT INTO orders (order_id, customer_id, order_date) "
                               "VALUES (3,8000,'2016/04/19'); ")

        self.my_cursor.execute("INSERT INTO orders (order_id, customer_id, order_date) "
                               "VALUES (4,4000,'2016/04/20'); ")

        self.my_cursor.execute("INSERT INTO orders (order_id, customer_id, order_date) VALUES (5,null,'2016/05/01'); ")

        self.my_cursor.execute("INSERT INTO products (product_id, product_name, category_id) VALUES (1,'Pear',50); ")

        self.my_cursor.execute("INSERT INTO products (product_id, product_name, category_id) VALUES (2,'Banana',50); ")

        self.my_cursor.execute("INSERT INTO products (product_id, product_name, category_id) VALUES (3,'Orange',50); ")

        self.my_cursor.execute("INSERT INTO products (product_id, product_name, category_id) VALUES (4,'Apple',50); ")

        self.my_cursor.execute("INSERT INTO products (product_id, product_name, category_id) VALUES (5,'Bread',75); ")

        self.my_cursor.execute(
            "INSERT INTO products (product_id, product_name, category_id) VALUES (6,'Sliced Ham',25); ")

        self.my_cursor.execute(
            "INSERT INTO products (product_id, product_name, category_id) VALUES (7,'Kleenex',null); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (100, 'Microsoft', 'Redmond', 'Washington');")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (200, 'Google', 'Mountain View', 'California'); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (300, 'Oracle', 'Redwood City', 'California');")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (400, 'Kimberly-Clark', 'Irving', 'Texas'); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (500, 'Tyson Foods', 'Springdale', 'Arkansas'); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (600, 'SC Johnson', 'Racine', 'Wisconsin'); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (700, 'Dole Food Company', 'Westlake Village', 'California'); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (800, 'Flowers Foods', 'Thomasville', 'Georgia'); ")

        self.my_cursor.execute("INSERT INTO suppliers (supplier_id, supplier_name, city, state) "
                               "VALUES (900, 'Electronic Arts', 'Redwood City', 'California'); ")

        self.my_cursor.execute("INSERT INTO summary_data (product_id, current_category) VALUES (1,10); ")

        self.my_cursor.execute("INSERT INTO summary_data (product_id, current_category) VALUES (2,10); ")

        self.my_cursor.execute("INSERT INTO summary_data (product_id, current_category) VALUES (3,10); ")

        self.my_cursor.execute("INSERT INTO summary_data (product_id, current_category) VALUES (4,10); ")

        self.my_cursor.execute("INSERT INTO summary_data (product_id, current_category) VALUES (5,10); ")

        self.my_cursor.execute("INSERT INTO summary_data (product_id, current_category) VALUES (8,10);")
        self.mydb.commit()
        
        self.my_cursor.execute('UPDATE summary_data SET current_category = (SELECT category_id'
                               'FROM products WHERE products.product_id = summary_data.product_id)'
                               'WHERE EXISTS (SELECT category_id FROM products '
                               'WHERE products.product_id = summary_data.product_id)')

    def display_db(self):
        my_result = self.my_cursor.fetchall()
        for x in my_result:
            print(x)


db = Connection('localhost', 'kagyrydb', 'mydatabase', 'M-7katerinah')
db.server_connection()
db.db_operation()
db.display_db()
