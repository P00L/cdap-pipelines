# Data Fusion CI/CD

Coming soon detailed article....

TODO architecture overview of the deployment pipeline

## Repository structure

```bash 
├── artifact
│   ├── <artifact-name>
│   │   ├── <artifact-name>-<artifact-version>.jar
│   │   └── <artifact-name>-<artifact-version>.json
├── pipeline
├── plugin
├── profile 
├── cloudbuild.yaml
├── datafusion.py
├── trigger.py
├── deploy.py
├── df_parameter.yaml
├── README.md
├── requirements.txt
├── trigger.yaml
```

- `artifact` folder containing [driver](https://cloud.google.com/data-fusion/docs/how-to/using-jdbc-drivers) and [plugin](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/480379172/Plugins) 
(`.jar` and `.json` file) deployed on data fusion, as today multiple artifact versions are supported only with multiple deployments, MUST
exist only one version for each artifact stored in this folder
- `pipeline` folder containing export of [pipeline](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/382141432/Exporting+and+importing+data+pipelines) (`.json` file) deployed on data fusion
- `profile` folder containing export of [compute profile](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/480314003/Profiles) (`.json` file) deployed on data fusion, for better management of secrets this folder can be retrieved from GCS based on the namespace
- `cloudbuild.yaml` [google cloud build](https://cloud.google.com/cloud-build/docs/build-config) deployment configuration file 
- `datafusion.py` python utility class fo data fusion communication
- `trigger.py` python script for automated creation of trigger.yaml script (contains list of pipelines in order of trigger)
- `deploy.py` python script for data fusion pipeline and trigger deployment 
- `df_parameter.yaml` yaml file containing data fusion secrets, for better management of secrets this folder can be retrieved from GCS based on the namespace
- `README.md` it's me
- `requirements.txt` python dependencies for `deploy.py` and `datafusion.py`
- `trigger.yaml` configuration file for [schedules/triggers](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/480346341/Schedules) deployment

## Deployment

As today deployment script manage:

- namespace
- compute profile
- artifact
- parameter
- secure parameter
- pipeline
- trigger

If a resource is not available the resource will be automatically created by the deployment script.

Deployment is managed by a python script, which entrypoint is `deploy.py`, that can be executed locally or by cloud build 
base on `cloudbuild.yaml` configuration. Following the steps performed by deployment script 

- check if namespace exist otherwise create it
- check if user (not system) compute profile in folder `profile` exist otherwise create it
- check if artifact in `artifact` folder exist otherwise create create it (new version or new version and artifact)
- updated parameter and secure parameter based on `df_paramenter.yaml`  
- Get pipeline deployed on data fusion
- Get pipeline pipeline stored in the repository in `pipeline` folder
- Diff of pipeline deployed on data fusion and stored in the repository to identify action on pipeline Create, Update Delete
- [OPTIONAL] update pipeline versions
- Execution of Create, Update, Delete actions for each pipeline based on the output of previous step
- For each pipeline Delete all trigger except for default one 
- Base on configuration (`trigger.yaml` file) Create trigger

#### Artifact configuration

Artifact folder is compose by a list of folder each of them containing jar and json file for source code and configuration

```
├── artifact
│   ├── <artifact-name>
│   │   ├── <artifact-name>-<artifact-version>.jar
│   │   └── <artifact-name>-<artifact-version>.json
│   ├── <artifact-name>
...
```

`json` file has the following keys, for details refer to the [official documentaion](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/477692148/Artifact+HTTP+RESTful+API)

| field      | description                      |
|------------|----------------------------------|  
| properties | dict of artifact definition      |
| parents    | list of parents for the artifact |
| plugins    | dict of plugin components        |

#### Trigger configuration

`trigger.yaml` file contains definition of schedule/trigger for each pipeline, It's a *YAML* with a list of object each of them
containing a trigger definition according to the following structure

```yaml
sourcePipelineName: name of the pipeline triggering the pipeline (this is not the pipeline for which we are deploying the trigger but the one that trigger it)
targetPipelineName: name of the pipeline triggered by the trigger (this is the pipeline for which we are deploying the trigger not the one triggerinf it)
pipelineStatus: list of status triggering the event [COMPLETED, FAILED, KILLED]
macrosMapping: OPTIONAL list of source/target macros mapping
```

For example following the definition of a trigger on target_pipeline that will be triggered by the finish of the execution of pipeline source_pipeline with COMPLETED status passing my_parameter macro. 

```yaml
- sourcePipelineName: source_pipeline
  targetPipelineName: target_pipeline
  pipelineStatus:
    - COMPLETED
  macrosMapping:
    - source: my_parameter
      target: my_parameter
```

#### Script environment variables

| var                | description                      |
|--------------------|----------------------------------|  
| DF_ENDPOINT        | data fusion endpoint https://\<df instance name\>-\<gcp project\>-dot-<gcp region short notation (ex. euw3)>.datafusion.googleusercontent.com/api|
| NAMESPACE          | namespace where pipeline and trigger should be deployed |
| PIPELINE_FOLDER    | folder containing pipeline `.json` export, default `pipeline` |
| UPGRADE_PIPELINE   | whether to update or not pipeline version |
| OVERWRITE_PIPELINE | whether to reflect or not pipeline upgrade on local json file |
| DF_VERSION         | data fusion version (may be retrieved directly from data fusion) |
| FORCE_DELETE       | flag that state if on pipeline update should be deleted and recreated instead of only updated, this is useful to force system preference update on pipeline parameter |
| LOG_LEVEL          | script log level allowed valued are python log level |

For local deployment [gcloud CLI](https://cloud.google.com/sdk/gcloud) should be set up 

```bash
# login with provided GCP credential
gcloud auth login

# set default project
gcloud config set project <PROJECT_ID>

# generate simulated service account for python SDK authentication
gcloud auth application-default login

# execute script
python deploy.py
```

#### Cloud Build

Cloud build helps automating process of release creating start pipeline CLoud Function and synchronizing data fusion pipelines
and trigger. 

##### Env var

| Env Var             | Description |
|---------------------|-------------|
| _DF_ENDPOINT        | data fusion endpoint https://\<df instance name\>-\<gcp project\>-dot-<gcp region short notation (ex. euw3)>.datafusion.googleusercontent.com/api |
| _DF_VERSION         | data fusion version |
| _NAMESPACE          | namespace where pipeline and trigger should be deployed \[dev, test, prod\] |
| _PIPELINE_FOLDER    | folder containing pipeline `.json` export, default `pipeline` |
| _SECRET_BUCKET      | bucket managing secrets, compute profile and data fusion parameters |  
| _UPGRADE_PIPELINE   | flag that state if upgrade pipeline version or not |
| _FORCE_DELETE       | flag that state if on pipeline update should be deleted and recreated instead of only updated, this is useful to force system preference update on pipeline parameter |

#### Secret management

For a better management of secrets `profile` folder and `df_paramenter.yaml` file will be stored in GCS in the relative *namespace* 
folder. For example GCS bucket <SECRET_BUCKET> will have the following strucuture 

```
├── namespace1
│   ├── profile
│   │   ├── profile1.json
│   │   └── profile2.json
│   └── df_parameter.yaml
├── namespace2
...
```

For local test this file should be downloaded or created manually. If the number of artifact increase needs to be evaluated to
download the artifact from GCS as well or to build locally from source maybe creating two release pipelines one for artifact and one
for pipelines

### Naming convention

- **pipeline export json**  \<name of the pipeline\>-cdap-data-pipeline.json

    name of the pipeline should be with snake_case convention
- **trigger/schedule**  \<target pipeline name\>.\<namespace\>.\<source pipeline name\>.\<namespace\>
    
    where \<source pipeline name\> is the pipeline that fire the event to start the \<target pipeline name\>
- **artifact** \<artifact name\>-<artifact version>

    name of the artifact should be with kebab-case convention
    
## References


- [CDAP documentation](https://cdap.atlassian.net/wiki/home)
- [CDAP medium documentation](https://medium.com/cdapio)
- [CDAP REST API documentation](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/477593807/CDAP+HTTP+RESTful+API+v3)
- [Data Fusion documentation](https://cloud.google.com/data-fusion/docs/how-to)
- [CI/CD CDAP pt.1](https://medium.com/cdapio/ci-cd-and-change-management-for-pipelines-part-1-1b4100aef66a)
- [CI/CD CDAP pt.2](https://medium.com/cdapio/ci-cd-and-change-management-for-pipelines-part-2-a286e806c2f2)
- [CI/CD CDAP pt.3](https://medium.com/cdapio/ci-cd-and-change-management-for-pipelines-part-3-be2e217b897f)
- [CI/CD CDAP pt.4 TBD]()
- [REST API CDAP pipeline deploy](https://medium.com/cdapio/deploying-and-running-cdf-pipelines-with-rest-1d469a243bd0)
- [Python GCP Bearer token](https://stackoverflow.com/questions/53472429/how-to-get-a-gcp-bearer-token-programmatically-with-python/53472880)
- [Data Fusion CDAP API documentation](https://cloud.google.com/data-fusion/docs/reference/cdap-reference)
- [CDAP secure parameter](https://medium.com/cdapio/securely-storing-sensitive-information-and-using-it-in-cdap-6bbafa96597d)
- [Resuse dataproc cluster](https://stackoverflow.com/questions/56873909/how-do-i-configure-cloud-data-fusion-pipeline-to-run-against-existing-hadoop-clu)
- [Terraform support for Data Fusion](https://cloud.google.com/blog/products/data-analytics/open-source-etl-pipeline-tool)