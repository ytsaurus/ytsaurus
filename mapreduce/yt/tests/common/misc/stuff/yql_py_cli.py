import json
import logging
import os
import sys
import traceback


from yql.api.v1.client import YqlClient
from yql.client.session import session
from yql.client.operation import YqlSqlOperationRequest, YqlOperationPlanRequest, YqlOperationAstRequest, YqlOperationResultsRequest
from yql.client.explain import YqlFormatExplainMixin
import yql_utils


def run_aux_request(request, field):
    progress = request.wait_progress()
    if progress == YqlOperationResultsRequest.WAIT_SUCCESS:
        data = request.json.get(field)
        if data:
            return data
        elif 'errors' in request.json.keys():
            request.exit_code = 53
            return YqlFormatExplainMixin.format_errors(request)
        else:
            return request.pretty
    else:
        return request.handle_progress(request.operation_id, progress)


def yql_exec(server, port, token=None, program=None, files=None, urls=None, run_sql=False, run_mkql=False,
             verbose=True, res_dir=None, check_error=True, pass_token_to_yt=False, attrs={}):
    if verbose:
        yql_utils.log('\tPROGRAM:\n' + program)
        logging.basicConfig(
            level=logging.DEBUG,
            stream=sys.stderr
        )
        requests_log = logging.getLogger('requests.packages.urllib3')
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    cli = YqlClient(
        server=server,
        token=token,
        port=port,
    )
    cli.config.no_color = True
    cli.config.pass_token_to_yt = pass_token_to_yt
    session.attached_files = []
    YqlSqlOperationRequest.attached_files = []

    request = cli.query(program, sql=run_sql)
    request.timeout = 15  # increase timeout
    request.additional_attributes.update(attrs)
    if verbose:
        yql_utils.log('\tadditional attrs:\n%s' % request.additional_attributes)

    if run_mkql:
        request.type = 'MKQL'  # TODO

    if files:
        for f in files:
            request.attach_file(files[f], f)

    if urls:
        for f in urls:
            request.attach_url(urls[f], f)

    request.run(force=True)

    if request.exc_info:
        assert 0, 'Have some problems:\n\n' + \
            ''.join(traceback.format_exception(*request.exc_info)) + '\n\n' + str(request.text)

    assert request.operation_id, str(request) + str(request.get_results())

    if verbose:
        yql_utils.log('\tOPERATION ID: ' + request.operation_id)

    if res_dir is None:
        res_dir = yql_utils.get_yql_dir(prefix='yql_exec_')

    def write_res_file(name, content):
        res_file = os.path.join(res_dir, name)
        with open(res_file, 'w') as f:
            f.write(content)
        return res_file

    if verbose:
        yql_utils.log('\tRAW RESPONSE:\n' + json.dumps(request.get_results().json, indent=4))

    res_file = write_res_file('results.json', json.dumps(request.get_results().json, indent=4))

    plan = run_aux_request(YqlOperationPlanRequest(request.operation_id, silent=True), 'plan')
    plan = YqlFormatExplainMixin.format_plan(plan) if isinstance(plan, dict) else plan
    ast = run_aux_request(YqlOperationAstRequest(request.operation_id, silent=True), 'ast')

    plan_file = write_res_file('plan.yson', plan.encode('utf8', 'replace')) if plan else None
    opt_file = write_res_file('opt.yql', ast)

    errors = '\n'.join([str(error) for error in request.errors])
    if check_error:
        assert not errors, errors

    return yql_utils.YQLExecResult(
        '',  # stdout
        errors,
        request.get_results().json,
        res_file,
        ast,
        opt_file,
        plan,
        plan_file,
        program,
        request
    )
