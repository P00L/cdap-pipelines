import os
import yaml
import logging
import google.auth
import google.auth.transport.requests
from datafusion import DataFusion
from distutils.util import strtobool

DF_ENDPOINT = os.environ['DF_ENDPOINT']
NAMESPACE = os.environ['NAMESPACE']
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
PIPELINE_FOLDER = os.environ.get('PIPELINE_FOLDER', 'pipeline')
UPGRADE_PIPELINE = bool(strtobool(os.environ['UPGRADE_PIPELINE']))
OVERWRITE_PIPELINE = bool(strtobool(os.environ.get('OVERWRITE_PIPELINE', 'false')))
FORCE_DELETE = bool(strtobool(os.environ.get('FORCE_DELETE', 'false')))

logging.basicConfig(level=logging.getLevelName(LOG_LEVEL))
logger = logging.getLogger(__name__)


class Credential:
    def __init__(self, ):
        """
        initialize credential and token for data fusion communication
        """
        self.__creds, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        self.__creds.refresh(auth_req)
        self.__token = self.__creds.token

    def get_token(self):
        """
        getting OAuth2 google token with auto-refresh when expired

        Returns:
            str: OAuth2 google token
        """
        if not self.__creds.valid or self.__creds.token is None or self.__creds.expired:
            logger.info('refresh token')
            auth_req = google.auth.transport.requests.Request()
            self.__creds.refresh(auth_req)
            self.__token = self.__creds.token
        return self.__token


def main():
    """
    Sync:
        - namespace
        - compute profile
        - artifact (driver and plugin) as today only netezza driver supported
        - parameter and secure parameter
        - pipeline
        -trigger

    Returns:

    """
    # TODO clean up code oganizing it with function
    repository_pipelines = [f.split('-')[0] for f in os.listdir(PIPELINE_FOLDER)]
    logger.debug(f'repository pipeline {repository_pipelines}')

    creds = Credential()

    data_fusion = DataFusion(namespace=NAMESPACE, df_endpoint=DF_ENDPOINT, creds=creds,
                             repository_pipelines=repository_pipelines, pipeline_folder=PIPELINE_FOLDER)

    # check existence of namespace, and in case create
    logger.info('********** sync namespace **********')
    if NAMESPACE not in data_fusion.get_namespaces():
        logger.warning(f'namespace {NAMESPACE} not available, will be created')
        data_fusion.create_namespace()

    # check existence of compute profile, and in case create
    logger.info('********** sync profile **********')
    repository_profiles = [f.split('.')[0] for f in os.listdir('profile')]
    deployed_profiles = data_fusion.get_profiles()
    for profile in repository_profiles:
        if profile not in deployed_profiles:
            logger.warning(f'profile {profile} not available, will be created')
        data_fusion.create_update_profile(profile=profile)

    # check existence of artifact, and in case create
    logger.info('********** sync artifact **********')
    deployed_artifacts = data_fusion.get_artifacts()
    repository_artifact = [f for f in os.listdir('artifact')]
    for artifact in repository_artifact:
        artifact_version = os.path.splitext(os.listdir(f'artifact/{artifact}')[0])[0].split('-')[-1]
        if artifact not in deployed_artifacts.keys():
            logger.warning(f'artifact {artifact} not available, will be created')
            data_fusion.create_artifact(name=artifact, version=artifact_version)
        else:
            if artifact_version not in deployed_artifacts[artifact]:
                logger.warning(f'artifact {artifact} version {artifact_version} not available, will be created')
                data_fusion.create_artifact(name=artifact, version=artifact_version)

    # check existence of parameter, and in case create
    logger.info('********** sync parameter **********')
    yaml_file = open('df_parameter.yaml')
    parameters = yaml.safe_load(yaml_file)
    for parameter in parameters:
        if parameter['type'] == 'secure':
            data_fusion.create_update_secure_parameter(name=parameter['name'], value=parameter['value'])
        else:
            data_fusion.create_update_parameter(values=parameter['values'])

    # sync pipeline from local repository to data fusion instance
    logger.info('********** sync pipeline **********')
    # update data fusion class object also with deployed artifact
    data_fusion.set_artifacts()
    data_fusion.sync_pipeline(upgrade_pipeline=UPGRADE_PIPELINE, overwrite_file=OVERWRITE_PIPELINE,
                              force_delete=FORCE_DELETE)

    # sync scheduled trigger from local repository to data fusion instance
    logger.info('********** sync trigger **********')
    data_fusion.sync_trigger()


if __name__ == "__main__":
    main()
