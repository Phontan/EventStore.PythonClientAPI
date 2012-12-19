import json

def convert_to_builtin_type(obj):
     d ={}
     d.update(obj.__dict__)
     return d

def to_json(obj):
    return json.dumps(obj, default=convert_to_builtin_type)