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

def is_positive_number(obj, arg):
    is_number(obj, arg)
    if obj<1:
        raise TypeError(arg +" cannot be negative or null")

def is_not_negative_number(obj, arg):
    is_number(obj, arg)
    if obj<0:
        raise TypeError(arg +" cannot be negative")

def is_possible_event_position(obj, arg):
    is_number(obj, arg)
    if obj<-1:
        raise TypeError(arg +" cannot be less than -1")

def is_bool(obj, arg):
    if type(obj) is not bool:
        raise TypeError(arg+" should be bool")