import os
import hailjwt as hj

GITHUB_CLONE_URL = 'https://github.com/'

with open(os.environ['HAIL_TOKEN_FILE']) as f:
    userdata = hj.JWTClient.unsafe_decode(f.read())
    BUCKET = f'gs://{userdata["bucket_name"]}'

AUTHORIZED_USERS = {
    'danking',
    'cseed',
    'konradjk',
    'jigold',
    'jbloom22',
    'patrick-schultz',
    'lfrancioli',
    'akotlar',
    'tpoterba',
    'chrisvittal',
    'catoverdrive',
    'daniel-goldstein',
    'ahiduchick',
    'GreatBrando',
    'johnc1231'
}
