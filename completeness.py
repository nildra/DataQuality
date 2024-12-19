file_path = 'data.csv'

point_count = 0
miss_count = 0
special_count = 0

def is_incomplete(point):             
    if len(point) == 0:           #if len == 0 then one column is empty so this line is not complete
        return True
    return False

def is_special(point):
    if (point < 0):
        return True
    return False 

with open(file_path, mode='r', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    header = next(csv_reader)  

    for row in csv_reader:             #we look for each line if it's complete
        for i, column in enumerate(row):
            point_count += 1
            if is_incomplete(column):
                miss_count += 1
            if isinstance(column, float):
                if is_special(column):
                    special_count += 1
    
    completeness = 1 - (miss_count + special_count) * 1.0/ (point_count + miss_count)
    print ("the completeness of this file is:", completeness*100, "%.")