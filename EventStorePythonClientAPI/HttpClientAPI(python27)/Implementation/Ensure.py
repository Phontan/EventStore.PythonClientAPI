def IsString(obj, arg):
    if type(obj)!=str:
        raise TypeError(arg +" should be string")
def IsNotEmptyString(obj, arg):
    IsString(obj, arg)
    if obj == "":
        raise TypeError(arg +" cannot be empty")
def IsFunction(obj, arg):
    if not hasattr(obj, '__call__'):
        raise TypeError(arg +" should be function")
def IsNumber(obj, arg):
    if type(obj)!= int:
        raise TypeError(arg +" should be integer")
def IsPositiveNumber(obj, arg):
    IsNumber(obj, arg)
    if obj<1:
        raise TypeError(arg +" cannot be negative or null")
def IsNotNegativeNumber(obj, arg):
    IsNumber(obj, arg)
    if obj<0:
        raise TypeError(arg +" cannot be negative")

