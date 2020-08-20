from json import dumps, loads
from requests import post
from flask import Response
from utils.configmanager import ConfigManager
from utils.rediscache import make_redis

conf = ConfigManager.get_config_value('ory')
apis = ConfigManager.get_config_value('apis')
hydra_config = conf['oauth2']['hydra']
oauth2_client = conf['oauth2']['client']

API_KEY = apis['ticketai']['apikey']

HYDRA_HOST = hydra_config['host']
HYDRA_PUBLIC_PORT = hydra_config['public_port']
HYDRA_ADMIN_PORT = hydra_config['admin_port']

def http_json_response_cache(r=None):
    def _wrapper(f):
        r = r or make_redis()
        def __wrapper(code, *args, **kwargs):
            if r.exists(code):
                cached = r.get(code).decode('utf-8')
                status, data = cached.split('\n')
                status = int(status)
                data = json.loads(data)
            else:
                status, data = f(code, *args, **kwargs)
                if status == 200:
                    serialized = f'{status}\n{json.dumps(data)}'
                    expires = int(data.get('expires_in', 3599))
                    r.set(code, serialized, ex=expires)
            return status, data
        return __wrapper
    return _wrapper

@http_json_response_cache
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

def require_auth(request, auth_type='oauth2'):
    def _wrapper(fn):
        def _validate_oauth2(*args, **kwargs):
            authorization = request.headers.get('Authorization')
            if authorization == None:
                return Response(None, status=403)

            auth_type, code = [h.strip() for h in authorization.split()]
            if auth_type != 'Bearer':
                return Response(None, status=403)

            status, data = __exchange_code(code)
            if status != 200:
                return Response(dumps(data), status=400, mimetype='application/json')

            status, data =  __token_introspection(data.get('access_token'))
            if status != 200:
                return Response(dumps(data), status=400, mimetype='application/json')

            claims = {
                'active': data['active'],
                'sub': data['sub'],
                'claims': data['ext']['claims']
            }
            return fn(*args, claims=claims, **kwargs)

        def _validate_apikey(*args, **kwargs):
            apikey = request.headers.get('X-API-KEY')
            if apikey == None:
                return Response(None, status=403)

            if apikey != API_KEY:
                return Response(None, status=403)
            return fn(*args, **kwargs)

        if auth_type == 'oauth2':
            return _validate_oauth2
        else:
            return _validate_apikey
    return _wrapper
