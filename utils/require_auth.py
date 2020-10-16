from re import compile
from json import dumps, loads
from requests import post, get
from flask import Response
from utils.configmanager import ConfigManager
from utils.rediscache import make_redis

conf = ConfigManager.get_config_value('ory')
hydra_config = conf['oauth2']['hydra']
oauth2_client = conf['oauth2']['client']

HYDRA_HOST = hydra_config['host']
HYDRA_PUBLIC_PORT = hydra_config['public_port']
HYDRA_ADMIN_PORT = hydra_config['admin_port']

def http_json_response_cache(r=None):
    r = r or make_redis()
    def _wrapper(f):
        def __wrapper(code, *args, **kwargs):
            if r.exists(code):
                cached = r.get(code).decode('utf-8')
                status, data = cached.split('\n')
                status = int(status)
                data = loads(data)
            else:
                status, data = f(code, *args, **kwargs)
                if status == 200:
                    serialized = f'{status}\n{dumps(data)}'
                    expires = int(data.get('expires_in', 3599))
                    r.set(code, serialized, ex=expires)
            return status, data
        return __wrapper
    return _wrapper

@http_json_response_cache()
def __exchange_code(code):
    data = {
        'grant_type':'authorization_code',
        'code': code,
        'redirect_uri': oauth2_client['redirect_uri'],
        'client_id': oauth2_client['client_id']
    }
    response = post(f'{HYDRA_HOST}:{HYDRA_PUBLIC_PORT}/oauth2/token', data=data, verify=False)
    return response.status_code, response.json()


def __token_introspection(access_token):
    data = {
        'token': access_token
    }
    response = post(f'{HYDRA_HOST}:{HYDRA_ADMIN_PORT}/oauth2/introspect', data=data, verify=False)
    return response.status_code, response.json()

def __userinfo(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = get(f'{HYDRA_HOST}:{HYDRA_PUBLIC_PORT}/userinfo', headers=headers, verify=False)
    return response.status_code, response.json()

def __validate_scope(claims, required_scope):
    matcher = compile(f'^{required_scope}.*')
    matched = map(matcher.match, claims)
    return any(matched)

def require_auth(request, auth_type='oauth2', required_scope: str = None):
    def _wrapper(fn):
        def _validate_oauth2(*args, **kwargs):
            authorization = request.headers.get('Authorization')
            if authorization == None:
                return Response(None, status=403)

            auth = [h.strip() for h in authorization.split()]

            if len(auth) < 2:
                return Response(None, status=401)

            authorization_type, code = auth
            if authorization_type != 'Bearer':
                return Response(None, status=401)

            status, data = __exchange_code(code)
            if status != 200:
                return Response(dumps(data), status=400, mimetype='application/json')

            access_token = data.get('access_token')
            status, data =  __token_introspection(access_token)
            if status != 200:
                return Response(dumps(data), status=400, mimetype='application/json')

            user_access = {
                'sub': data['sub'],
                'claims': data['ext']['claims'],
                'branch_id': data['ext']['bra'],
                'organization_id': data['ext']['org']
            }

            if required_scope:
                access = __validate_scope(user_access[claims], required_scope)
                if not access:
                    return Response(None, status=403)

            if auth_type == 'oauth2+openid':
                status, userinfo = __userinfo(access_token)
                return fn(*args, user_access=user_access, userinfo=userinfo, **kwargs)
            return fn(*args, user_access=user_access, **kwargs)

        return _validate_oauth2
    return _wrapper
