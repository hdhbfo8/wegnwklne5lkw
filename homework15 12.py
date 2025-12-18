def multiply_list(lst):

    if not lst:
        return 0
    
    result = 1
    for num in lst:
        result *= num
    return result




def multiply_list(numbers):
       
    result = 1
    for num in numbers:
        result *= num
    return result

num = [1, 3, 3, 5, 6, 7, 8, 8, 9]
uniq = set(num)

print(uniq)




def compare_lists(list1, list2):  
    for item in list1:  
        if item in list2:  
            return True  
    return False  
 