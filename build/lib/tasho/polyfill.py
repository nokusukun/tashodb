# Polyfill layer for Python 2 compatibility.

try:
	import secrets
	hex_token = secrets.token_hex
except:
	import random
	import string
	hex_token = lambda x=32: "".join([random.choice(string.hexdigits.lower()) for y in range(0, x * 2)])
