import datetime


class Employee:
    # Class variables are shared among all instances of the class.
    raise_amount = 1.5

    # Instance Method
    def __init__(self, first_name, last_name, pay):
        self.first_name = first_name
        self.last_name = last_name
        self.pay = pay

    @property
    def email(self):
        return '{}.{}@company.com'.format(self.first_name, self.last_name)

    @property
    def full_name(self):
        return '{} {}'.format(self.first_name, self.last_name)

    @full_name.setter
    def full_name(self, name):
        self.first_name, self.last_name = name.split(' ')

    @full_name.deleter
    def full_name(self):
        print('Delete Name')
        self.first_name = None
        self.last_name = None

    def get_full_name(self):
        return '{} {}'.format(self.first_name, self.last_name)

    def apply_raise(self):
        self.pay = int(self.pay * self.raise_amount)

    @classmethod
    def set_raise_amount(cls, amount):
        cls.raise_amount = amount

    # Alternative Constructor
    @classmethod
    def construct_from_string(cls, emp_str):
        first_name, last_name, pay = emp_str.split('-')
        pay = int(pay)
        return cls(first_name, last_name, pay)

    @staticmethod
    def is_weekday(day):
        if day.weekday() == 5 or day.weekday() == 6:
            return False
        return True

    def __repr__(self):
        return 'Employee() {} {} {}'.format(self.first_name, self.last_name, self.pay)

    def __str__(self):
        return '{} {} {}'.format(self.first_name, self.last_name, self.pay)

    def __add__(self, other):
        return self.pay + other.pay


class Developer(Employee):
    raise_amount = 2.10

    def __init__(self, first_name, last_name, pay, programing_language):
        super().__init__(first_name, last_name, pay)
        self.programing_language = programing_language


class Manager(Employee):
    raise_amount = 0.50

    def __init__(self, first_name, last_name, pay, employees: list = None):
        super().__init__(first_name, last_name, pay)
        if employees is None:
            self.employees = []
        else:
            self.employees = employees

    def add_emp(self, emp):
        if emp not in self.employees:
            self.employees.append(emp)

    def remove_emp(self, emp):
        if emp in self.employees:
            self.employees.remove(emp)

    def print_emps(self):
        if self.employees is not None:
            for emp in self.employees:
                print('-->', emp.full_name)


if __name__ == '__main__':
    # Employee 1
    Employee.set_raise_amount(20)
    emp_1 = Employee('Ankit', 'Mishra', 5000)
    emp_1.apply_raise()
    print(emp_1.pay)
    employee_str_1 = 'Ankit-Mishra-6000'

    # Employee 3
    emp_3 = Employee.construct_from_string(employee_str_1)
    emp_3.apply_raise()
    print('Emp 3: First Name:', emp_3.first_name)
    print('Emp 3 Last Name:', emp_3.last_name)
    print('Emp3 Pay:', emp_3.pay)

    # Employee 2
    emp_2 = Employee('Amit', 'Mishra', 5000)
    emp_2.raise_amount = 10.20
    emp_2.apply_raise()
    print(emp_2.pay)
    print(emp_2.__dict__)

    # Check static method
    my_date = datetime.date(2023, 10, 18)
    is_weekday = Employee.is_weekday(my_date)
    print('is_weekday :', is_weekday)

    # Inheritance
    dev_1 = Developer('Ankit', 'Mishra', 5000, 'Python')
    dev_2 = Developer('Amit', 'Gupta', 7000, 'Java')
    dev_3 = Developer('Gyan', 'Singh', 9000, 'C++')

    mag_1 = Manager('Tarun', 'Verma', 10000, [dev_1, dev_2])
    mag_2 = Manager('WolfGang', 'Kochen', 9000, [dev_3])

    print(mag_1.print_emps())

    print(isinstance(dev_1, Employee))  # True
    print(isinstance(dev_1, Developer))  # True
    print(isinstance(dev_1, Manager))  # False
    print(issubclass(Developer, Manager))  # False
    print(issubclass(Manager, Employee))  # True

    # Special/Magic/ Dunder Method

    print(emp_1)
    print(emp_1.__repr__())
    print(emp_1 + emp_2)

    # Property Decorators
    print('==== Property Decorators =======')
    emp1 = Employee('Amit', 'Sharma', 8000)
    emp1.first_name = 'Tarun'
    print(emp1.full_name)
    print(emp1.email)
    emp_1.full_name = 'Arun Sharma'
    print(emp_1)

    del emp1.full_name
    print(emp1.first_name)
