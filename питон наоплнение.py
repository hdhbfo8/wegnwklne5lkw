# 1 задание
file_1 = open("text.txt")
read_file = file_1.read()
print(read_file)
file_1.close

# 2 задание
import os

print(os.listdir("C://"))

#3 задание 
def copy_numbers():
f1 = open('test1.txt', 'r')
f2 = open('test2.txt', 'w')

for word in f1.read().split():
if word.isdigit():
f2.write(word + '\n')

f1.close()
f2.close()

copy_numbers()