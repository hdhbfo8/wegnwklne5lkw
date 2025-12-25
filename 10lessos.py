# 1 задание

class One:
    def __init__(self, a, b):
        self.a = a
        self.b = b
    
    def addition(self):
        return self.a + self.b
    
    def subtraction(self):
        return self.a - self.b
    
    def multiplication(self):
        return self.a * self.b
    
    def division(self):
        return self.a / self.b
    
calc = One(7, 5)

print(f"Числа: a = {calc.a}, b = {calc.b}")
print(f"Сложение: {calc.addition()}")
print(f"Вычитание: {calc.subtraction()}")
print(f"Умножение: {calc.multiplication()}")
print(f"Деление: {calc.division()}")
    
# 2 задание
class Student:
    def __init__(self, name, number, age, group):
        self.name = name
        self.number = number
        self.age = age
        self.group = group

    def get_name(self):
        return self.name

    def get_age(self):
        return self.age

    def get_number(self):
        return self.number

    def set_age(self, new_age):
        self.age = new_age
        print(f"Возраст студента {self.name} изменен на {self.age}")

    def set_group(self, new_group):
        self.group = new_group
        print(f"Группа студента {self.name} изменена на {self.group}")

student1 = Student("Саша Сивенцев", "001", 18, "Э-101")

print(f"Студент: {student1.get_name()}")
print(f"Возраст: {student1.get_age()}")
print(f"ID: {student1.get_number()}")
