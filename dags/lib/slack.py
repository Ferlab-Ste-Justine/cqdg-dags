import json
import urllib.parse
from lib import config
from lib import utils
from lib.config import env


class Slack:

    SUCCESS = ':large_green_square:'
    INFO = ':large_blue_square:'
    WARNING = ':large_orange_square:'
    ERROR = ':large_red_square:'

    def notify(markdown: str, type=INFO):
        if config.slack_hook_url:
            airflow_link = f' *[*<{config.slack_hook_url}|Airflow>*]*' if config.slack_hook_url else ''
            utils.http_post(config.slack_hook_url, {
                'blocks': [
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': f'{type} *{env.upper()}*{airflow_link} {markdown}',
                        },
                    },
                ],
            })

    def notify_task_failure(context):
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        dag_link = Slack._dag_link(
            f'{dag_id}.{task_id}', dag_id, context['run_id'], task_id,
        )
        Slack.notify(f'Task {dag_link} failed.', Slack.ERROR)

    def notify_dag_start(context):
        dag_id = context['dag'].dag_id
        dag_link = Slack._dag_link(dag_id, dag_id, context['run_id'])
        dag_params = Slack._dag_params(context['params'])
        Slack.notify(f'DAG {dag_link} started.{dag_params}', Slack.INFO)

    def notify_dag_completion(context):
        dag_id = context['dag'].dag_id
        dag_link = Slack._dag_link(dag_id, dag_id, context['run_id'])
        dag_params = Slack._dag_params(context['params'])
        Slack.notify(f'DAG {dag_link} completed.{dag_params}', Slack.SUCCESS)

    def _dag_link(text: str, dag_id: str, run_id: str = '', task_id: str = ''):
        if config.slack_hook_url:
            params = urllib.parse.urlencode({
                'dag_run_id': run_id,
                'task_id': task_id
            })
            return f'<{config.slack_hook_url}/dags/{dag_id}/grid?{params}|{text}>'
        return text

    def _dag_params(params: dict):
        if params:
            params_json = json.dumps(params, indent=4)
            return f'```{params_json}```'
        return ''