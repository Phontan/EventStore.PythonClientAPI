def is_string(obj, arg):
    if type(obj)!=str:
        raise TypeError(arg +" should be string")

def is_not_empty_string(obj, arg):
    is_string(obj, arg)
    if obj == "":
        raise TypeError(arg +" cannot be empty")

def is_function(obj, arg):
    if not hasattr(obj, '__call__'):
        raise TypeError(arg +" should be function")

def is_number(obj, arg):
    if type(obj)!= int:
        raise TypeError(arg +" should be integer(not {0}".format(obj))

def is_greater_number_then(number, obj, arg):
    is_number(obj, arg)
    if obj<number:
        raise TypeError("{0} cannot be less than {1}".format(arg, number))

def is_bool(obj, arg):
    if type(obj) is not bool:
        raise TypeError(arg+" should be bool")