def _python_type_to_schema_type(p):
    if p == 'str':
        return 'string'
    elif p in ['int', 'float']:
        return 'number'
    elif p in {'typing.List[str]', 'typing.Tuple[str]', 'list', 'tuple'}:
        return 'array'
    elif p == 'bool':
        return 'boolean'
    elif p == 'dict':
        return 'object'
    else:
        return 'null'
            # raise TypeError(f'{p} is not supported')


def _cli_to_schema(
    api_dict,
    target,
    extras=None,
    required=None,
    allow_addition=False,
    namespace='Jina',
    description='',
):
    deployment_api = []

    if not isinstance(target, list):
        target = [target]

    for d in api_dict['methods']:
        if d['name'] in target:
            deployment_api.extend(d['options'])

    _schema = {
        'properties': {},
        'type': 'object',
        'required': [],
        'additionalProperties': allow_addition,
        'description': description,
    }

    for d in deployment_api:
        dtype = _python_type_to_schema_type(d['type'])
        pv = {'description': d['help'].strip(), 'type': dtype, 'default': d['default']}
        if d['choices']:
            pv['enum'] = d['choices']
        if d['required']:
            _schema['required'].append(d['name'])
        if dtype == 'array':
            _schema['items'] = {'type': 'string', 'minItems': 1, 'uniqueItems': True}

        _schema['properties'][d['name']] = pv

    if extras:
        _schema['properties'].update(extras)
    if required:
        _schema['required'].extend(required)

    return {f'{namespace}::{target[0].capitalize()}': _schema}
