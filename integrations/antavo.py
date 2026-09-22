"""Antavo API signing and reward-catalog client."""
import hashlib
import hmac
import json
import os
import re
from datetime import datetime, timezone
import requests


def _antavo_date():
    return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def _sha256_hex(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _hmac_sha256(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def sign_antavo_request(method, stack, uri, parameters, api_key, api_secret, payload=None):
    host = f'api.{stack}.antavo.com'
    date = _antavo_date()
    date_part = date.split('T')[0]
    is_body_method = method.lower() in ('post', 'put', 'patch')
    body_str = json.dumps(payload) if is_body_method and payload is not None else ''
    sorted_parameters = '&'.join(sorted(parameters.split('&'))) if parameters else ''
    canonical_headers = f'date:{date}\nhost:{host}\n'
    canonical_request = (
        f'{method.upper()}\n{uri}\n{sorted_parameters}\n'
        f'{canonical_headers}\ndate;host\n{_sha256_hex(body_str)}'
    )
    scope = f'{date_part}/{stack}/api/antavo_request'
    string_to_sign = f'ANTAVO-HMAC-SHA256\n{date}\n{scope}\n{_sha256_hex(canonical_request)}'
    key = _hmac_sha256(('ANTAVO' + api_secret).encode('utf-8'), date_part)
    for component in (stack, 'api', 'antavo_request'):
        key = _hmac_sha256(key, component)
    signature = hmac.new(key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
    headers = {
        'date': date,
        'Authorization': f'ANTAVO-HMAC-SHA256 Credential={api_key}/{scope}, SignedHeaders=date;host, Signature={signature}',
    }
    if is_body_method:
        headers['Content-Type'] = 'application/json'
    url = f'https://{host}{uri}'
    if sorted_parameters:
        url += '?' + sorted_parameters
    return url, headers, body_str


class AntavoError(Exception):
    pass


def fetch_rewards():
    stack = os.environ.get('ANTAVO_STACK', '')
    api_key = os.environ.get('ANTAVO_API_KEY', '')
    secret = os.environ.get('ANTAVO_API_SECRET', '')
    if not re.fullmatch(r'[a-z0-9-]+', stack) or not api_key or not secret:
        raise AntavoError('Set ANTAVO_STACK, ANTAVO_API_KEY and ANTAVO_API_SECRET.')
    url, headers, _ = sign_antavo_request(
        'GET', stack, '/entities/rewards/reward', '', api_key, secret
    )
    try:
        # Do not forward signed credentials through redirects.
        response = requests.get(url, headers=headers, timeout=(5, 20), allow_redirects=False)
    except requests.RequestException:
        raise AntavoError('Antavo connection failed or timed out.') from None
    if response.status_code != 200:
        # Avoid echoing credentials or raw upstream error pages.
        raise AntavoError(f'Antavo returned HTTP {response.status_code}. Check endpoint access and signing credentials.')
    try:
        data = response.json()
    except ValueError:
        raise AntavoError('Antavo returned a non-JSON response.') from None
    if not isinstance(data, (dict, list)):
        raise AntavoError('Antavo returned an unexpected JSON value.')
    # Preserve the actual envelope: the docs show an object despite describing a list.
    return data
