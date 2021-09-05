from typing import List

from apache_beam.internal.gcp.auth import _Credentials, _GceAssertionCredentials, is_running_in_gce # noqa
import logging

from google.auth import impersonated_credentials
from oauth2client.client import GoogleCredentials


DATAFLOW_REQUIRED_SCOPE = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/datastore',
    'https://www.googleapis.com/auth/spanner.admin',
    'https://www.googleapis.com/auth/spanner.data'
]


def _generate_impersonated_credentials(service_account: str,
                                       specified_scope=None):
    if specified_scope is None:
        specified_scope = DATAFLOW_REQUIRED_SCOPE
    if is_running_in_gce:
        return _GceAssertionCredentials(user_agent='beam-python-sdk/1.0')
    else:
        try:
            source_credentials = GoogleCredentials.get_application_default()
        except Exception as e:
            logging.warning(
                'Unable to find default credentials to use: %s\n'
                'Connecting anonymously.',
                e)
            return None

        try:
            target_credentials = impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=service_account,
                target_scopes=specified_scope)

            logging.debug(f'Connecting using Google Application Default and impersonate to {service_account}')
            return target_credentials
        except Exception as e:
            logging.warning(
                f'Fail to impersonate to {service_account}: %s\n'
                'Will fallback to the original behavior on apache beam.',
                e)
            return None


def force_impersonating(service_account: str,
                        specified_scope: List[str] = None):
    """
    Helper function to impersonating the giving service account.

    Args:
        service_account: The target service account.
        specified_scope: Specified required scope. Defaults to DATAFLOW_REQUIRED_SCOPE
    """
    if not is_running_in_gce:
        if _Credentials._credentials_init:  # noqa
            logging.warning(
                'Detected the credential has been created, but it is not create by force_impersonating function.'
            )
            logging.debug(
                'The function is preparing for uploading dataflow template by impersonated service account, '
                'it required to be ran before running the pipeline to make sure the credential has been created before '
                'the sdk generate it by itself. In the common situation, you do not need to execute this function. ')
        else:

            with _Credentials._credentials_lock:  # noqa
                _Credentials._credentials = _generate_impersonated_credentials(service_account, specified_scope)
                _Credentials._credentials_init = True
