import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import account_drive as d
import hyeoks_account as a
from test_account_nav import fixture


def service(permission='user',folder=False):
    s=MagicMock()
    s.files().get().execute.return_value={'mimeType':'application/vnd.google-apps.folder' if folder else 'application/json','size':'10'}
    s.permissions().list().execute.return_value={'permissions':[{'type':permission,'role':'owner'}]}
    return s


class StorageTests(unittest.TestCase):
    def test_service_account_rejected_before_remote_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'credentials.json';p.write_text(json.dumps({'type':'service_account'}))
            with self.assertRaises(ValueError):d.credential_info(p)

    def test_authorized_user_requires_refresh_material(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'credentials.json'
            info={'type':'authorized_user','client_id':'test','client_secret':'test','refresh_token':'test'}
            p.write_text(json.dumps(info))
            self.assertEqual(d.credential_info(p),info)
            del info['refresh_token'];p.write_text(json.dumps(info))
            with self.assertRaises(ValueError):d.credential_info(p)

    def test_public_destination_rejected(self):
        with self.assertRaises(ValueError):d.restricted(service('anyone',True),'folder',True)

    def test_domain_destination_rejected(self):
        with self.assertRaises(ValueError):d.restricted(service('domain',True),'folder',True)

    def test_group_destination_rejected(self):
        with self.assertRaises(ValueError):d.restricted(service('group',True),'folder',True)

    def test_permissions_pagination_checked(self):
        s=service();s.permissions().list().execute.side_effect=[{'permissions':[{'type':'user'}],'nextPageToken':'x'},{'permissions':[{'type':'anyone'}]}]
        with self.assertRaises(ValueError):d.restricted(s,'id')

    def test_unverifiable_acl_rejected(self):
        s=service();s.permissions().list().execute.return_value={}
        with self.assertRaises(ValueError):d.restricted(s,'id')

    def test_shared_drive_rejected_until_policy_supported(self):
        s=service();s.files().get().execute.return_value={'driveId':'shared','mimeType':'application/json'}
        with self.assertRaises(ValueError):d.restricted(s,'id')

    def test_input_download_exclusive(self):
        s=service();s.files().get_media().execute.return_value=json.dumps(fixture()).encode()
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'input.json';d.download(s,'id',p)
            with self.assertRaises(FileExistsError):d.download(s,'id',p)

    def test_live_google_sheet_not_accepted_as_frozen_json(self):
        s=service();s.files().get().execute.return_value={'mimeType':'application/vnd.google-apps.spreadsheet'}
        with self.assertRaises(ValueError):d.download(s,'id','unused')

    def test_local_tampering_aborts_archive(self):
        b=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            p=a.save_bundle(b,a.calculate(b),tmp)
            (p/'input.json').write_text('{}')
            with self.assertRaises(ValueError):d.archive_run(p)

    def test_incomplete_local_run_cannot_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):d.archive_run(tmp)

    def test_private_upload_readback_success(self):
        s=service(folder=True);s.files().create().execute.return_value={'id':'saved'}
        s.files().get_media().execute.return_value=b'archive'
        with tempfile.TemporaryDirectory() as tmp,patch.object(d,'archive_run',return_value=b'archive'):
            receipt=d.upload_verified(s,'folder',tmp,MagicMock())
            self.assertEqual(receipt['status'],'remote_verified')
            self.assertTrue((Path(tmp)/'remote_receipt.json').exists())

    def test_corrupt_remote_readback_has_no_success_receipt(self):
        s=service(folder=True);s.files().create().execute.return_value={'id':'saved'}
        s.files().get_media().execute.return_value=b'corrupt'
        with tempfile.TemporaryDirectory() as tmp,patch.object(d,'archive_run',return_value=b'archive'):
            with self.assertRaises(ValueError):d.upload_verified(s,'folder',tmp,MagicMock())
            self.assertFalse((Path(tmp)/'remote_receipt.json').exists())

    def test_cli_full_frozen_input_save(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'in.json';source.write_text(json.dumps(fixture()),encoding='utf-8')
            output=Path(tmp)/'output'
            self.assertEqual(a.main(['--input',str(source),'--output-dir',str(output)]),0)
            self.assertEqual(len(list(output.glob('*/COMPLETE'))),1)


if __name__=='__main__':unittest.main()
