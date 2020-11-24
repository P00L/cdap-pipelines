import os
import json
import yaml
import logging
import requests
from custom_exception import MissingTriggerPipeline, MissingArtifact

logger = logging.getLogger(__name__)

DF_VERSION = os.environ['DF_VERSION']


class DataFusion:
    def __init__(self, namespace, df_endpoint, creds, repository_pipelines, pipeline_folder):
        """
        https://cdap.atlassian.net/wiki/spaces/DOCS/pages/477593807/CDAP+HTTP+RESTful+API+v3
        # TODO may be created a method for api invocation to keep the code cleaner
        Args:
            namespace (str): namespace where pipelines and triggers will be deployed
            df_endpoint (str): data fusion endpoint
            creds (Credential): credential object holding OAuth2 google token
            repository_pipelines (list of str): list of pipeline (json file) locally on the repository at pipeline_folder path
            pipeline_folder (str): path where to retrieve locally pipeline
        """
        self.namespace = namespace
        self.df_endpoint = df_endpoint
        self.creds = creds
        self.repository_pipelines = repository_pipelines
        self.pipeline_folder = pipeline_folder
        self.artifacts = {}

    def get_pipelines(self):
        """
        Retrieve deployed pipeline
        GET /v3/namespaces/<namespace-id>/apps

        Returns:
            (list of str): list of pipeline deployed on data fusion
        """
        logger.info('getting pipelines')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps'
        response = requests.get(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e
        return [p['name'] for p in response.json()]

    def get_namespaces(self):
        """
        Retrieve data fusion namespaces
        GET /v3/namespaces

        Returns:
            (list of str): list of namespaces on data fusion
        """
        logger.info('getting namespace')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces'
        response = requests.get(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e
        return [n['name'] for n in response.json()]

    def create_namespace(self):
        """
        Create data fusion namespaces
        PUT /v3/namespaces/<namespace-id>

        Returns:
        """
        logger.info(f'create namespace {self.namespace}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}'
        response = requests.put(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def get_profiles(self):
        """
        Retrieve data user fusion compute profile
        GET /v3/profiles

        Returns:
            (list of str): list of profiles on data fusion
        """
        logger.info('getting profiles')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/profiles'
        params = {'includeSystem': False}
        response = requests.get(endpoint, headers=headers, params=params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e
        return [p['name'] for p in response.json()]

    def create_update_profile(self, profile):
        """
        Create or update data fusion user compute profile
        PUT /v3/namespaces/<namespace-id>/profiles/<profile-name>

        Args:
            profile (str): name of the compute profile to be created

        Returns:
        """
        logger.info(f'Create/update profile {profile}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/profiles/{profile}'

        json_file = open(f'profile/{profile.split("-")[0]}.json')
        profile_config = json.load(json_file)

        profile_config['name'] = profile
        profile_config['label'] = profile

        response = requests.put(endpoint, headers=headers, data=json.dumps(profile_config))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def get_artifacts(self, scope='USER'):
        """
        Retrieve data fusion artifact (driver, plugin)
        GET /v3/namespaces/<namespace-id>/artifacts[?scope=<scope>]

        Args:
            scope (str): artifact scopes to be retrieved
                allowed values [USER, SYSTEM, ALL]

        Returns:
            (dict): data fusion plugin name and version list
                example {'argument-setter-plugins': ['1.1.0','1.1.1']}
        """
        logger.info('getting artifacts')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        if scope == 'ALL':
            params = {}
        else:
            params = {'scope': scope}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/artifacts'
        response = requests.get(endpoint, headers=headers, params=params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e
        artifacts = {}
        for artifact in response.json():
            artifacts.setdefault(artifact['name'], []).append(artifact['version'])
        return artifacts

    def set_artifacts(self):
        """
        Set class artifacts parameter
        Returns:

        """
        # TODO evaluate if needs to be ordered the list of versions
        self.artifacts = self.get_artifacts(scope='ALL')

    def create_artifact(self, name, version):
        """
        Create data fusion artifact
        POST /v3/namespaces/<namespace-id>/artifacts/<artifact-name>

        Args:
            name (str): name of teh artifact to be created
            version (str): version of the artifact to be created

        Returns:

        """
        logger.info(f'create artifact {name}-{version}')
        plugin_config = {None}
        file_path = f'artifact/{name}/{name}-{version}.json'
        if os.path.exists(file_path):
            json_file = open(file_path)
            plugin_config = json.load(json_file)
        else:
            logger.warning(f'file {file_path} not exist')
        headers = {
            'Authorization': f'Bearer {self.creds.get_token()}',
            'Artifact-Version': version}
        if 'plugins' in plugin_config:
            headers['Artifact-Plugins'] = json.dumps(plugin_config['plugins'])
        if 'parents' in plugin_config:
            headers['Artifact-Extends'] = '/'.join(plugin_config['parents'])

        data = open(f'artifact/{name}/{name}-{version}.jar', 'rb').read()
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/artifacts/{name}'
        response = requests.post(endpoint, headers=headers, data=data)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

        if 'properties' in plugin_config:
            logger.info(f'attach properties to artifact {name}-{version}')
            headers = {
                "Authorization": f"Bearer {self.creds.get_token()}"
            }
            endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/artifacts/{name}/versions/{version}/properties'
            response = requests.put(endpoint, headers=headers, data=json.dumps(plugin_config['properties']))
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.exception(e)
                raise e

    def create_update_secure_parameter(self, name, value, description=None):
        """
        Create/update secure parameter
        PUT /v3/namespaces/<namespace-id>/securekeys/<secure-key-id>

        ATTENTION!!! update on parameters are ignored for deployed pipeline. To force pipeline parameter update
        use the force_delete flag on pipeline sync

        Args:
            name (str): name of the secure parameter
            value (str): value of the secure parameter

        Returns:

        """
        logger.info(f'Create/update secure parameter {name}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/securekeys/{name}'
        body = {
            "description": description,
            "data": value
        }
        response = requests.put(endpoint, headers=headers, data=json.dumps(body))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def create_update_parameter(self, values):
        """
        Create/update secure parameter
        PUT /v3/namespaces/<namespace-id>/preferences

        ATTENTION!!! update on parameters are ignored for deployed pipeline. To force pipeline parameter update
        use the force_delete flag on pipeline sync
        Args:
            values (str):

        Returns:

        """
        logger.info(f'Create/update parameters')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/preferences'
        body = {}
        for value in values:
            body[value['name']] = value['value']
        response = requests.put(endpoint, headers=headers, data=json.dumps(body))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def create_pipeline(self, pipeline_name, body):
        """
        Create pipeline
        PUT /v3/namespaces/<namespace-id>/apps/<app-id>

        Args:
            body (dict): dictionary containing pipeline definition according to API specification
            pipeline_name (str): name of the pipeline to be deployed

        Returns:

        """
        logger.info(f'create pipeline {pipeline_name}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{pipeline_name}'
        response = requests.put(endpoint, headers=headers, data=json.dumps(body))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def update_pipeline(self, pipeline_name, body):
        """
        Update existing pipeline
        POST /v3/namespaces/<namespace-id>/apps/<app-id>/update

        Args:
            body (dict): dictionary containing pipeline definition according to API specification
            pipeline_name (str): name of the pipeline to be updated

        Returns:

        """
        logger.info(f'update pipeline {pipeline_name}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{pipeline_name}/update'
        response = requests.post(endpoint, headers=headers, data=json.dumps(body))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def delete_pipeline(self, pipeline_name):
        """
        Delete existing pipeline
        DELETE /v3/namespaces/<namespace-id>/apps/<app-id>

        Args:
            pipeline_name (str): name of the pipeline to be deleted

        Returns:

        """
        logger.info(f'delete pipeline {pipeline_name}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{pipeline_name}'
        response = requests.delete(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def sync_pipeline(self, upgrade_pipeline=False, delete_pipeline=True, overwrite_file=False, force_delete=False):
        """
        Sync exported pipeline to data fusion instance
        TODO evaluate to use commit diff to deploy only changed pipeline

        Args:
            upgrade_pipeline (bool): flag indicating if pipeline plugin and data fusion version should be matched with
                the ones deployed on data fusion namespace
                default False
            delete_pipeline (bool): flag indicating if pipeline not store on repository but deployed in data fusion
                should be deleted
                default True
            overwrite_file (bool): flag indicating during pipeline upgrade json configuration file should be stored
                locally. This flag is ignored if sync upgrade_pipeline flag is set to False
                default False
            force_delete (bool): flag indicating if instead of updating pipeline, delete and recreate from scratch
                default False
        Returns:


        """
        deployed_pipelines = self.get_pipelines()

        for p in self.repository_pipelines:
            json_file = open(f'{self.pipeline_folder}/{p}-cdap-data-pipeline.json')
            pipeline_config = json.load(json_file)
            if upgrade_pipeline:
                self.update_pipeline_versions(pipeline_name=p, pipeline_config=pipeline_config,
                                              overwrite_file=overwrite_file)

            if p in deployed_pipelines:
                if force_delete:
                    self.delete_pipeline(pipeline_name=p)
                    self.create_pipeline(pipeline_name=p, body=pipeline_config)
                else:
                    self.update_pipeline(pipeline_name=p, body=pipeline_config)
            else:
                self.create_pipeline(pipeline_name=p, body=pipeline_config)
        if delete_pipeline:
            for p in set(deployed_pipelines) - set(self.repository_pipelines):
                self.delete_pipeline(pipeline_name=p)

    def update_pipeline_versions(self, pipeline_name, pipeline_config, overwrite_file=False):
        """
        Update pipeline plugin and data fusion versions matching with the ones deployed on data fusion namespace.
        The configuration is updated in-place. If artifact is missing MissingArtifact exception is raised. If enabled
        json file for the pipeline will be  overwritten, this functionality is very useful during upgrade process
        ATTENTION updating pipeline version may cause errors due to incompatibility of plugin
        Args:
            pipeline_name (str): name of the pipeline to be updated
            pipeline_config (dict): pipeline configuration json
            overwrite_file (bool): flag to enable or disable overwrite json file
                default: False
        Returns:

        """
        if pipeline_config['artifact']['version'] != DF_VERSION:
            logger.warning(f'upgrade data fusion version from {pipeline_config["artifact"]["version"]} to {DF_VERSION}')
            pipeline_config['artifact']['version'] = DF_VERSION
        for stage in pipeline_config['config']['stages']:
            if stage['plugin']['artifact']['name'] not in self.artifacts:
                error_message = f"missing artifact {stage['plugin']['artifact']['name']}"
                logger.error(error_message)
                raise MissingArtifact(error_message)
            if stage['plugin']['artifact']['version'] not in self.artifacts[stage['plugin']['artifact']['name']]:
                logger.warning(
                    f"upgrade plugin {stage['plugin']['artifact']['name']} from {stage['plugin']['artifact']['version']} to {self.artifacts[stage['plugin']['artifact']['name']][-1]}")
                stage['plugin']['artifact']['version'] = self.artifacts[stage['plugin']['artifact']['name']][-1]
        if overwrite_file:
            file_path = f'{self.pipeline_folder}/{pipeline_name}-cdap-data-pipeline.json'
            logger.info(f'overwrite file {file_path}')
            with open(file_path, 'w') as outfile:
                json.dump(pipeline_config, outfile, indent=4)

    def get_trigger(self, pipeline_name):
        """
        Retrieve pipeline schedules except of default one dataPipelineSchedule
        GET /v3/namespaces/<namespace-id>/apps/<app-id>/schedules

        Args:
            pipeline_name (str): name of the pipeline to be deleted

        Returns:

        """
        logger.info(f'get pipeline {pipeline_name} trigger')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{pipeline_name}/schedules'
        response = requests.get(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e
        triggers = [t['name'] for t in response.json()]
        triggers.remove('dataPipelineSchedule')
        return triggers

    def get_trigger_body(self, trigger_config, trigger_name):
        """
        Build body request for pipeline trigger (schedule) create and update.
        Only trigger schedule supported without and/or condition

        Args:
            trigger_config (dict): configuration of the trigger. It's an entry of the trigger.yaml file containing basic configuration of the trigger
            trigger_name (str): name of the trigger <sourcePipelineName>.<namespace>.<targetPipelineName>.<namespace>

        Returns:
            dict: body representation of the trigger for create and update API
        """
        data = {
            "name": trigger_name,
            "description": "",
            "namespace": self.namespace,
            "application": trigger_config["targetPipelineName"],
            "applicationVersion": "-SNAPSHOT",
            "program": {
                "programName": "DataPipelineWorkflow",
                "programType": "WORKFLOW"
            },
            "properties": {
                "triggering.properties.mapping": json.dumps(
                    {"arguments": trigger_config['macrosMapping'] if 'macrosMapping' in trigger_config else [],
                     "pluginProperties": []}),
            },
            "trigger": {
                "type": "PROGRAM_STATUS",
                "programId": {
                    "namespace": self.namespace,
                    "application": trigger_config["sourcePipelineName"],
                    "version": "-SNAPSHOT",
                    "type": "WORKFLOW",
                    "entity": "PROGRAM",
                    "program": "DataPipelineWorkflow"
                },
                "programStatuses": trigger_config['pipelineStatus'],
            }
        }
        return data

    def create_trigger(self, trigger_config):
        """
        Create pipeline trigger schedule. Once created the trigger is disabled by default and should be manually enabled

        PUT  /v3/namespaces/<namespace-id>/apps/<app-id>/schedules/<schedule-id>
        POST /v3/namespaces/<namespace-id>/apps/<app-id>/schedules/<schedule-id>/enable
        Args:
            trigger_config (dict): configuration of the trigger. It's an entry of the trigger.yaml file containing basic configuration of the trigger

        Returns:

        """
        trigger_name = f'{trigger_config["targetPipelineName"]}.{self.namespace}.{trigger_config["sourcePipelineName"]}.{self.namespace}'
        logger.info(f'create trigger {trigger_name} for pipeline {trigger_config["targetPipelineName"]}')

        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{trigger_config["targetPipelineName"]}/schedules/{trigger_name}'

        response = requests.put(endpoint, headers=headers,
                                data=json.dumps(self.get_trigger_body(trigger_config, trigger_name)))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e
        # activate endpoint
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{trigger_config["targetPipelineName"]}/schedules/{trigger_name}/enable'
        response = requests.post(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def update_trigger(self, trigger_config):
        """
        Update existing trigger schedule

        POST /v3/namespaces/<namespace-id>/apps/<app-id>/schedules/<schedule-id>/update
        Args:
            trigger_config (dict): configuration of the trigger. It's an entry of the trigger.yaml file containing basic configuration of the trigger

        Returns:

        """
        trigger_name = f'{trigger_config["targetPipelineName"]}.{self.namespace}.{trigger_config["sourcePipelineName"]}.{self.namespace}'
        logger.info(f'update trigger {trigger_name} for pipeline {trigger_config["targetPipelineName"]}')

        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{trigger_config["targetPipelineName"]}/schedules/{trigger_name}/update'

        response = requests.post(endpoint, headers=headers,
                                 data=json.dumps(self.get_trigger_body(trigger_config, trigger_name)))
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def delete_trigger(self, pipeline_name, trigger_name):
        """
        Delete existing pipeline
        /v3/namespaces/<namespace-id>/apps/<app-id>/schedules/<schedule-id>

        Args:
            pipeline_name (str): name of the pipeline to be deleted
            trigger_name (str): name of  the trigger to be deleted

        Returns:

        """
        logger.info(f'delete trigger {trigger_name} for pipeline {pipeline_name}')
        headers = {"Authorization": f"Bearer {self.creds.get_token()}"}
        endpoint = f'{self.df_endpoint}/v3/namespaces/{self.namespace}/apps/{pipeline_name}/schedules/{trigger_name}'
        response = requests.delete(endpoint, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(e)
            raise e

    def sync_trigger(self):
        """
        Sync pipeline trigger schedule to data fusion.
        Trigger configuration is managed by trigger.yaml file, composed by a list of trigger with the following structure:

            sourcePipelineName: name of the pipeline triggering the pipeline (this is not the pipeline for which we are deploying the trigger but the one that trigger it)
            targetPipelineName: name of the pipeline triggered by the trigger (this is the pipeline for which we are deploying the trigger not the one triggerinf it)
            pipelineStatus: list of status triggering the event [COMPLETED, FAILED, KILLED]
            macrosMapping: OPTIONAL list of source/target macros mapping

        Example
            - sourcePipelineName: source_pipeline
              targetPipelineName: target_pipeline
              pipelineStatus:
                - COMPLETED
              macrosMapping:
                - source: current_date
                  target: current_date

        For now only one trigger for each pipeline can be specified
        TODO add possibility to define multiple trigger for a pipeline (this constraint is in contrast with delete of schedule, a solution may be define an outer level for the pipeline with all trigger)
        TODO add possibility to define complex trigger, for example a trigger type in the ocnfiguration

        Returns:

        """
        yaml_file = open('trigger.yaml')
        triggers = yaml.safe_load(yaml_file)
        deployed_pipelines = self.get_pipelines()
        for t in triggers:

            if t["targetPipelineName"] not in deployed_pipelines or t["sourcePipelineName"] not in deployed_pipelines:
                error_message = f'missing one of [{t["targetPipelineName"]}, {t["sourcePipelineName"]}] pipeline'
                logger.error(error_message)
                raise MissingTriggerPipeline(error_message)

            pipeline_trigger = self.get_trigger(pipeline_name=t["targetPipelineName"])

            # clean up trigger
            for pt in pipeline_trigger:
                self.delete_trigger(pipeline_name=t["targetPipelineName"], trigger_name=pt)

            # create trigger
            self.create_trigger(trigger_config=t)
