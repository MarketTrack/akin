from akin import Akin
from http import HTTPStatus
from flask import Blueprint, current_app, request, jsonify
from flask.blueprints import BlueprintSetupState
from werkzeug.utils import secure_filename
from typing import Callable, Tuple
import os

blueprint = Blueprint('ui', __name__)

_akin: Akin = None


@blueprint.record_once
def record(setup_state: BlueprintSetupState):
    global _akin
    try:
        _akin = setup_state.options['akin']
    except KeyError as e:
        raise InitializationError(f"missing option: {e}")


@blueprint.route('/index')
@blueprint.route('/')
def index():
    from flask import render_template
    return render_template('home.html.j2', akin=_akin)


@blueprint.route('/query', methods = ['GET', 'POST'])
def query():
    if request.method == 'GET':
        from flask import render_template
        return render_template('query.html.j2', akin=_akin)
    else:
        datasource_name = request.json.get('datasource_name')
        group_names = request.json.get('group_names')
        query = request.json.get('query')

        return jsonify(_akin.query_group(datasource_name, group_names, query))


@blueprint.route('/delete_datasource')
def delete_dataset():
    dataset_to_delete = request.args.get('datasource')
    return combine_result(*_akin.delete_datasource(dataset_to_delete))


@blueprint.route('/get_datasets')
def get_datasets():
    datasets = jsonify([dsk for dsk, dkv in _akin.datasources.items()])
    return datasets


@blueprint.route('/get_groups/<dsname>')
def get_groups(dsname):
    groups = jsonify([])
    datasource = _akin.datasources.get(dsname)
    if datasource:
        groups = jsonify([g.field for g in datasource.groups.values()])
    return groups


@blueprint.route('/get_fields/<dsname>')
def get_fields(dsname):
    fields = jsonify([])
    datasource = _akin.datasources.get(dsname)
    if datasource and datasource.data:
        fields = jsonify([h for h in datasource.data[0].keys() if not h.startswith('__')])
    return fields


@blueprint.route('/create_group')
def create_group():
    dsname = request.args.get('dataset')
    field_name = request.args.get('field')
    group_template = request.args.get('group_template')

    datasource = _akin.datasources.get(dsname)
    group_settings = _akin.grouptemplates.get(group_template)
    if datasource and datasource.data and group_settings:
        _akin.group_data(datasource, field_name, group_settings)
    return jsonify({'success':True})


@blueprint.route('/get_group_data')
def get_group_data():    
    dsname = request.args.get('dataset')
    group_name = request.args.get('group')

    data_entries = list()
    headers = list()
    datasource = _akin.datasources.get(dsname)
    if datasource:
        group = datasource.groups.get(group_name)
        if group:
            #h.encode('ascii', 'ignore').decode('ascii')
            headers = [h for h in datasource.data[0].keys()]
            # Establish ids for the groups for display purposes
            group_id = 0
            for gl in group.values:
                group_id += 1
                for g in gl:
                    g['group_id'] = group_id
            data_entries = [item for sublist in group.values for item in sublist]
            if data_entries:
                headers = [h for h in data_entries[0].keys() if not h.startswith('_') and not h.startswith('\ufeff')]
                data_entries = [[dv for dk, dv in de.items() if not dk.startswith('_') and not dk.startswith('\ufeff')] for de in data_entries]
            return jsonify({'headers': headers, 'data': data_entries})


@blueprint.route('/uploadfile', methods = ['POST'])
def uploadfile():
    import csv

    f = request.files['file']
    filename = secure_filename(f.filename)
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(os.path.dirname(filepath)):
        try:
            os.makedirs(os.path.dirname(filepath))
        except OSError as exc: # Guard against race condition
            import errno
            if exc.errno != errno.EEXIST:
                raise
    
    f.save(filepath)

    all_data_entries = []
    with open(filepath, 'r', encoding='utf8') as data_file:
        for row in csv.DictReader(data_file, dialect="excel"):
            all_data_entries.append(row)

    return combine_result(*_akin.add_datasource(filename, all_data_entries))


def combine_result(success: bool, result: str) -> Tuple[object, HTTPStatus]:
    return result, HTTPStatus.OK if success else HTTPStatus.BAD_REQUEST


class InitializationError(RuntimeError):
    pass