"""Restricted Drive input/output for manual account workflow. No public artifacts.

Uses authorized-user OAuth, not an unauthenticated GAS upload URL.
It never changes permissions. It refuses public/domain/group and shared-drive
destinations, then checks uploaded permissions and full content readback.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import zipfile


def credential_info(path):
    info = json.loads(Path(path).read_text(encoding='utf-8'))
    if info.get('type') != 'authorized_user' or any(not info.get(k) for k in ('client_id','client_secret','refresh_token')):
        raise ValueError('authorized-user OAuth credentials required; service accounts cannot own My Drive files')
    return info


def restricted(service, file_id, folder=False):
    meta = service.files().get(fileId=file_id, fields='id,mimeType,driveId,trashed,size').execute()
    if meta.get('trashed') or meta.get('driveId'):
        raise ValueError('trashed/shared-drive destination is unsupported')
    if folder and meta['mimeType'] != 'application/vnd.google-apps.folder':
        raise ValueError('destination must be a folder')
    token = None
    count = 0
    while True:
        response = service.permissions().list(fileId=file_id,pageSize=100,pageToken=token,
                                               fields='nextPageToken,permissions(type,role)').execute()
        for permission in response.get('permissions',[]):
            count += 1
            if permission.get('type') != 'user':
                raise ValueError('only explicit user permissions are permitted')
        token = response.get('nextPageToken')
        if not token: break
    if not count:
        raise ValueError('permissions could not be verified')
    return meta


def download(service, file_id, path):
    meta = restricted(service,file_id)
    if meta.get('mimeType','').startswith('application/vnd.google-apps.'):
        raise ValueError('input must be a frozen JSON file, not a live Google document')
    if int(meta.get('size',0)) > 100_000_000:
        raise ValueError('input exceeds 100 MB')
    data = service.files().get_media(fileId=file_id).execute()
    if len(data) > 100_000_000:
        raise ValueError('input exceeds 100 MB')
    value = json.loads(data)
    if value.get('schema') not in ('account-input-v2','account-input-v3'):
        raise ValueError('unexpected input schema')
    path = Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as stream: stream.write(data)


def archive_run(run_dir):
    run_dir = Path(run_dir)
    if not (run_dir/'COMPLETE').is_file():
        raise ValueError('incomplete local run')
    manifest = json.loads((run_dir/'manifest.json').read_text(encoding='utf-8'))
    allowed = {'input.json','result.json','report.md','hyeoks_account.py','hyeoks_verdict.py'}
    if set(manifest['files']) != allowed:
        raise ValueError('unexpected manifest files')
    for name,checksum in manifest['files'].items():
        if hashlib.sha256((run_dir/name).read_bytes()).hexdigest()!=checksum:
            raise ValueError('local file checksum mismatch')
    buf = io.BytesIO()
    with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(allowed|{'manifest.json','COMPLETE'}):
            archive.writestr(name,(run_dir/name).read_bytes())
    return buf.getvalue()


def upload_verified(service, folder_id, run_dir, media_factory):
    restricted(service,folder_id,folder=True)
    data = archive_run(run_dir)
    response = service.files().create(
        body={'name':Path(run_dir).name+'.zip','parents':[folder_id]},
        media_body=media_factory(io.BytesIO(data),mimetype='application/zip',resumable=False),
        fields='id').execute()
    file_id = response['id']
    restricted(service,file_id)
    saved = service.files().get_media(fileId=file_id).execute()
    if hashlib.sha256(saved).digest()!=hashlib.sha256(data).digest():
        raise ValueError('remote content verification failed')
    # A receipt contains identifiers/hashes, never copied to public output.
    receipt={'status':'remote_verified','file_id':file_id,'archive_sha256':hashlib.sha256(data).hexdigest()}
    with (Path(run_dir)/'remote_receipt.json').open('x',encoding='utf-8') as stream:
        json.dump(receipt,stream)
    return receipt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['download','upload'])
    parser.add_argument('--id',required=True,help='input file ID or restricted output folder ID')
    parser.add_argument('--path',required=True)
    parser.add_argument('--credentials',default='secret.json')
    args=parser.parse_args()
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        credentials=Credentials.from_authorized_user_info(credential_info(args.credentials),scopes=['https://www.googleapis.com/auth/drive'])
        service=build('drive','v3',credentials=credentials,cache_discovery=False)
        if args.action=='download':download(service,args.id,args.path)
        else:upload_verified(service,args.id,args.path,MediaIoBaseUpload)
    except Exception:
        # API errors can contain private names/IDs; do not echo them in public logs.
        print('ACCOUNT_STORAGE_FAILED: verify credentials, explicit-user ACL and input locally')
        return 2
    print('ACCOUNT_STORAGE_VERIFIED')
    return 0


if __name__=='__main__':raise SystemExit(main())
